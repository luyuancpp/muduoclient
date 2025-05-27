package muduo

import (
	"bytes"
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
	InputBuffer bytes.Buffer
	NeedClose   atomic.Bool
	MutexOut    sync.Mutex
	MutexIn     sync.Mutex
	OutMsgList  chan proto.Message
	InMsgList   chan proto.Message
	Codec       Codec
	Addr        string
}

func NewConnection(addr string, codec Codec) (*Connection, error) {
	conn, err := Connect(addr)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		Conn:       conn,
		OutMsgList: make(chan proto.Message, 100),
		InMsgList:  make(chan proto.Message, 100),
		Codec:      codec,
		Addr:       addr,
	}

	go c.HandleReadBufferFromConn()
	go c.HandleWriteMsgToBuffer()

	return c, nil
}

func GetDescriptor(m *proto.Message) protoreflect.MessageDescriptor {
	reflection := proto.MessageReflect(*m)
	return reflection.Descriptor()
}

func (c *Connection) HandleWriteMsgToBuffer() {
	for {
		msg := <-c.OutMsgList

		data, err := c.Codec.Encode(&msg)
		if err != nil {
			log.Println("Encode error:", err)
			continue
		}

		{
			c.MutexOut.Lock()
			defer c.MutexOut.Unlock()

			total := 0
			for total < len(data) {
				n, err := c.Conn.Write(data[total:])
				if err != nil {
					log.Println("Write error:", err)
					return
				}
				total += n
			}
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

func (c *Connection) HandleReadBufferFromConn() {
	buf := make([]byte, 1024)

	for {
		if c.NeedClose.Load() {
			return
		}

		n, err := c.Conn.Read(buf)
		if err != nil {
			if err == io.EOF || n == 0 {
				log.Println("Connection closed by peer, attempting reconnect")
				conn, err := Connect(c.Addr)
				if err != nil {
					log.Println("Reconnect failed:", err)
					return
				}
				c.Conn = conn
				continue
			}
			log.Println("Read error:", err)
			return
		}

		if n > 0 {
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

				// 移除已读字节
				_, err = c.InputBuffer.Read(make([]byte, msgLen))
				if err != nil {
					log.Println("InputBuffer read error:", err)
					break
				}

				c.InMsgList <- msg
			}
		}
	}
}
