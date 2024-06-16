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
	InputBuffer  bytes.Buffer
	NeedClose    atomic.Bool
	MutexOut     sync.Mutex
	MutexIn      sync.Mutex
	OutMsgList   chan proto.Message
	InMsgList    chan proto.Message
}

func GetDescriptor(m *proto.Message) protoreflect.MessageDescriptor {
	reflection := proto.MessageReflect(*m)
	return reflection.Descriptor()
}

func (c *Connection) HandleWriteMsgToBuffer() {
	for {
		m := <-c.OutMsgList
		data, err := Encode(&m)
		if err != nil {
			log.Println(err)
			return
		}
		c.writeToBuffer(data)
	}
}

func (c *Connection) writeToBuffer(data []byte) {
	c.MutexOut.Lock()
	defer c.MutexOut.Unlock()
	c.OutputBuffer.Write(data)
}

func (c *Connection) writeBufferToConn() {
	if c.OutputBuffer.Len() <= 0 {
		return
	}
	c.MutexOut.Lock()
	defer c.MutexOut.Unlock()
	_, err := c.OutputBuffer.WriteTo(c.conn)
	if err != nil {
		log.Println(err)
		return
	}
}

func (c *Connection) HandleWriteBufferToConn() {
	for {
		if c.NeedClose.Load() {
			return
		}
		c.writeBufferToConn()
	}
}

func (c *Connection) readBufferFromConn() {
	c.MutexIn.Lock()
	defer c.MutexIn.Unlock()
	_, err := c.InputBuffer.ReadFrom(c.conn)
	if err != nil {
		log.Println(err)
		return
	}
}

func (c *Connection) HandleReadMsgFromBuffer() {
	for {
		msg, msgLen, err := Decode(c.InputBuffer.Bytes())
		if err != nil {
			log.Println(err)
			return
		}
		if msgLen <= 0 {
			return
		}
		c.InputBuffer.Truncate(int(msgLen))
		list := c.InMsgList
		list <- msg
	}
}

func (c *Connection) HandleReadBufferFromConn() {
	for {
		if c.NeedClose.Load() {
			return
		}
		c.readBufferFromConn()
	}
}
