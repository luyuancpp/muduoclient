package muduo

import (
	"bytes"
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	addr  string
	codec Codec

	conn   net.Conn
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	incoming chan proto.Message
	outgoing chan proto.Message

	buffer      bytes.Buffer
	bufferMutex sync.Mutex

	closed     atomic.Bool
	writeMutex sync.Mutex
}

// NewConnection 创建连接并启动管理协程，自动重连
func NewConnection(addr string, codec Codec) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		addr:     addr,
		codec:    codec,
		ctx:      ctx,
		cancel:   cancel,
		incoming: make(chan proto.Message, 100),
		outgoing: make(chan proto.Message, 100),
	}
	c.wg.Add(1)
	go c.connectionManager()
	return c
}

// connectionManager 负责连接生命周期和重连逻辑
func (c *Connection) connectionManager() {
	defer c.wg.Done()
	for {
		if c.closed.Load() {
			return
		}

		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			log.Println("Connect failed:", err)
			time.Sleep(time.Second)
			continue
		}

		c.setConn(conn)
		log.Println("Connected to", c.addr)

		readCtx, readCancel := context.WithCancel(c.ctx)
		writeCtx, writeCancel := context.WithCancel(c.ctx)

		c.wg.Add(2)
		go c.readLoop(readCtx, readCancel)
		go c.writeLoop(writeCtx, writeCancel)

		// 等待读写协程退出（断线或关闭）
		c.wg.Wait()

		// 关闭当前连接
		c.closeConn()

		// 如果主动关闭，退出重连
		if c.closed.Load() {
			return
		}

		log.Println("Disconnected, retrying connection...")
		time.Sleep(time.Second)
	}
}

func (c *Connection) setConn(conn net.Conn) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = conn
}

func (c *Connection) closeConn() {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) readLoop(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	defer c.wg.Done()

	buf := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.writeMutex.Lock()
		conn := c.conn
		c.writeMutex.Unlock()
		if conn == nil {
			return
		}

		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed by server")
			} else {
				log.Println("Read error:", err)
			}
			return
		}
		if n > 0 {
			c.bufferMutex.Lock()
			c.buffer.Write(buf[:n])
			for {
				msg, msgLen, err := c.codec.Decode(c.buffer.Bytes())
				if err != nil {
					log.Println("Decode error:", err)
					break
				}
				if msgLen <= 0 {
					break
				}
				c.buffer.Next(int(msgLen)) // 移除已解析数据

				select {
				case c.incoming <- msg:
				default:
					log.Println("Incoming channel full, dropping message")
				}
			}
			c.bufferMutex.Unlock()
		}
	}
}

func (c *Connection) writeLoop(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.outgoing:
			if !ok {
				return
			}
			data, err := c.codec.Encode(&msg)
			if err != nil {
				log.Println("Encode error:", err)
				continue
			}

			c.writeMutex.Lock()
			conn := c.conn
			c.writeMutex.Unlock()
			if conn == nil {
				return
			}

			total := 0
			for total < len(data) {
				n, err := conn.Write(data[total:])
				if err != nil {
					log.Println("Write error:", err)
					return
				}
				total += n
			}
		}
	}
}

// Send 发送消息，非阻塞
func (c *Connection) Send(msg proto.Message) error {
	if c.closed.Load() {
		return errors.New("connection closed")
	}
	select {
	case c.outgoing <- msg:
		return nil
	default:
		return errors.New("send buffer full")
	}
}

// Recv 获取收到的消息，阻塞等待
func (c *Connection) Recv() (proto.Message, error) {
	msg, ok := <-c.incoming
	if !ok {
		return nil, errors.New("connection closed")
	}
	return msg, nil
}

// Close 主动关闭连接和所有协程
func (c *Connection) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.cancel()
		c.closeConn()
		close(c.outgoing)
		close(c.incoming)
		c.wg.Wait()
		log.Println("Connection closed cleanly")
	}
}
