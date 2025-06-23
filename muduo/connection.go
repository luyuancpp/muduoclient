package muduo

import (
	"bytes"
	"context"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	Conn        net.Conn
	Addr        string
	Codec       Codec
	InputBuffer bytes.Buffer
	bufferLock  sync.Mutex

	NeedClose  atomic.Bool
	MutexOut   sync.Mutex
	OutMsgList chan proto.Message
	InMsgList  chan proto.Message

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConnection(addr string, codec Codec) (*Connection, error) {
	conn, err := Connect(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Connection{
		Conn:       conn,
		Addr:       addr,
		Codec:      codec,
		OutMsgList: make(chan proto.Message, 100),
		InMsgList:  make(chan proto.Message, 100),
		ctx:        ctx,
		cancel:     cancel,
	}

	c.startLoops()
	return c, nil
}

func (c *Connection) startLoops() {
	c.wg.Add(2)
	go c.handleReadLoop()
	go c.handleWriteLoop()
}

func (c *Connection) Close() {
	if c.NeedClose.CompareAndSwap(false, true) {
		log.Println("Closing connection...")
		c.cancel()
		_ = c.Conn.Close()
		close(c.OutMsgList)
		close(c.InMsgList)
		c.wg.Wait()
		log.Println("Connection closed cleanly")
	}
}

func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	if m == nil {
		return nil
	}
	return proto.MessageReflect(m).Descriptor()
}

func (c *Connection) handleWriteLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Write loop exiting")
			return

		case msg, ok := <-c.OutMsgList:
			if !ok {
				log.Println("OutMsgList closed")
				return
			}

			data, err := c.Codec.Encode(&msg)
			if err != nil {
				log.Println("Encode error:", err)
				continue
			}

			c.MutexOut.Lock()
			if c.Conn == nil || c.NeedClose.Load() {
				log.Println("Write skipped: connection closed or nil")
				c.MutexOut.Unlock()
				return
			}

			total := 0
			for total < len(data) {
				n, err := c.Conn.Write(data[total:])
				if err != nil {
					log.Println("Write error:", err)
					c.MutexOut.Unlock()
					return
				}
				total += n
			}
			c.MutexOut.Unlock()
		}
	}
}

func (c *Connection) handleReadLoop() {
	defer c.wg.Done()
	buf := make([]byte, 1024)

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Read loop exiting")
			return
		default:
		}

		n, err := c.Conn.Read(buf)
		if err != nil {
			if err == io.EOF || n == 0 {
				log.Println("Disconnected, attempting reconnect...")

				// 停掉旧 goroutine
				c.cancel()
				c.wg.Wait()

				// 清空状态
				c.bufferLock.Lock()
				c.InputBuffer.Reset()
				c.bufferLock.Unlock()

				// 重新连接
				conn, err := Connect(c.Addr)
				if err != nil {
					log.Println("Reconnect failed:", err)
					return
				}
				c.Conn = conn

				// 重新启动 goroutine
				c.ctx, c.cancel = context.WithCancel(context.Background())
				c.startLoops()
				return
			}
			log.Println("Read error:", err)
			return
		}

		if n > 0 {
			c.bufferLock.Lock()
			c.InputBuffer.Write(buf[:n])
			for {
				msg, msgLen, err := c.Codec.Decode(c.InputBuffer.Bytes())
				if err != nil {
					log.Println("Decode error:", err)
					break
				}
				if msgLen <= 0 {
					break
				}

				_, err = c.InputBuffer.Read(make([]byte, msgLen))
				if err != nil {
					log.Println("InputBuffer read error:", err)
					break
				}

				select {
				case c.InMsgList <- msg:
				default:
					log.Println("InMsgList full, dropping message")
				}
			}
			c.bufferLock.Unlock()
		}
	}
}

func Connect(addr string) (net.Conn, error) {
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			log.Println("Connected to", addr)
			return conn, nil
		}
		log.Println("Connect failed:", err)
		time.Sleep(100 * time.Millisecond)
	}
}
