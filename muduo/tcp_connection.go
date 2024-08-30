package muduo

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	Conn         net.Conn
	OutputBuffer bytes.Buffer
	InputBuffer  bytes.Buffer
	NeedClose    atomic.Bool
	MutexOut     sync.Mutex
	MutexIn      sync.Mutex
	OutMsgList   chan proto.Message
	InMsgList    chan proto.Message
	Codec        Codec
	Addr         string
}

func GetDescriptor(m *proto.Message) protoreflect.MessageDescriptor {
	reflection := proto.MessageReflect(*m)
	return reflection.Descriptor()
}

func (c *Connection) HandleWriteMsgToBuffer() {
	for {
		m := <-c.OutMsgList

		data, err := c.Codec.Encode(&m)

		if err != nil {
			log.Println(err)
			return
		}

		c.OutputBuffer.Write(data)

		_, err = c.OutputBuffer.WriteTo(c.Conn)

		if err != nil {
			log.Println(err)
			return
		}
	}
}

func Connect(addr string) (net.Conn, error) {
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		fmt.Println("connect fail:", err)
		time.Sleep(100 * time.Millisecond)
	}
	return conn, nil
}

func (c *Connection) HandleReadBufferFromConn() {
	for {
		if c.NeedClose.Load() {
			return
		}

		data := make([]byte, 512)
		n, err := c.Conn.Read(data)

		if n == 0 {
			//golang tcp 断线重连
			conn, err := Connect(c.Addr)
			if err != nil {
				log.Println("reconnect fail:", err)
				return
			}
			c.Conn = conn
			return
		}

		if err != nil {
			log.Println(err)
			return
		}

		c.InputBuffer.Write(data[0:n])

		msg, msgLen, err := c.Codec.Decode(c.InputBuffer.Bytes())

		if err != nil {
			log.Println(err)
			return
		}

		if msgLen <= 0 {
			continue
		}

		_, err = c.InputBuffer.Read(make([]byte, msgLen))
		if err != nil {
			log.Println(err)
			return
		}

		c.InMsgList <- msg
	}
}
