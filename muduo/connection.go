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

func (c *Connection) connectionManager() {
	defer c.wg.Done()

	for {
		if c.IsClosed() {
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

		// 清空旧 buffer
		c.bufferMutex.Lock()
		c.buffer.Reset()
		c.bufferMutex.Unlock()

		readCtx, readCancel := context.WithCancel(c.ctx)
		writeCtx, writeCancel := context.WithCancel(c.ctx)

		var rwg sync.WaitGroup
		rwg.Add(2)
		go func() {
			defer rwg.Done()
			c.readLoop(readCtx, readCancel)
		}()
		go func() {
			defer rwg.Done()
			c.writeLoop(writeCtx, writeCancel)
		}()

		// 等待读写结束
		rwg.Wait()

		// 清理连接
		c.closeConn()

		// 如果是主动关闭则退出
		if c.IsClosed() {
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

		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue // 忽略超时，继续读取
			}
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
				c.buffer.Next(int(msgLen))
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
				log.Println("Write skipped: no active connection")
				return
			}

			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

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

func (c *Connection) Send(msg proto.Message) (err error) {
	if c.IsClosed() {
		return errors.New("connection closed")
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("send failed: connection closed")
		}
	}()
	select {
	case c.outgoing <- msg:
		return nil
	default:
		return errors.New("send buffer full")
	}
}

func (c *Connection) Recv() (proto.Message, error) {
	msg, ok := <-c.incoming
	if !ok {
		return nil, errors.New("connection closed")
	}
	return msg, nil
}

func (c *Connection) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.cancel()
		c.closeConn()
		// outgoing 关闭后，写协程才能退出
		close(c.outgoing)
		// 不关闭 incoming，避免 panic（由接收方决定关闭）
		c.wg.Wait()
		log.Println("Connection closed cleanly")
	}
}

func (c *Connection) IsClosed() bool {
	return c.closed.Load()
}
