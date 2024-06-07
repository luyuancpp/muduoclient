package muduo

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

type Connection struct {
	conn         net.Conn
	OutputBuffer bytes.Buffer
	NeedClose    atomic.Bool
	mu           sync.Mutex
	M            chan proto.Message
}

func GetDescriptor(m *proto.Message) protoreflect.MessageDescriptor {
	reflection := proto.MessageReflect(*m)
	return reflection.Descriptor()
}

func (c *Connection) HandleWriteMsgToBuffer() {
	for {
		m := <-c.M
		data, err := Encode(&m)
		if err != nil {
			log.Println(err)
			return
		}
		c.writeToBuffer(data)
	}
}

func (c *Connection) writeToBuffer(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.OutputBuffer.Write(data)
}

func (c *Connection) writeBufferToConn() {
	if c.OutputBuffer.Len() <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.OutputBuffer.WriteTo(c.conn)
	if err != nil {
		log.Println(err)
		return
	}
}

func (c *Connection) HandleWriteBufferToMsg() {
	for {
		if c.NeedClose.Load() {
			return
		}
		c.writeBufferToConn()
	}
}
