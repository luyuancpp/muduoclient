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
	Codec        Codec
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

func (c *Connection) readMsgFromBuff() {
	c.MutexIn.Lock()
	defer c.MutexIn.Unlock()
	msg, msgLen, err := c.Codec.Decode(c.InputBuffer.Bytes())
	if err != nil {
		log.Println(err)
		return
	}
	if msgLen <= 0 {
		return
	}
	_, err = c.InputBuffer.Read(make([]byte, msgLen))
	if err != nil {
		log.Println(err)
		return
	}
	c.InMsgList <- msg
}

func (c *Connection) readBufferFromConn() {
	data := make([]byte, 512)
	n, err := c.conn.Read(data)
	if n == 0 {
		c.NeedClose.Store(true)
		return
	}
	if err != nil {
		log.Println(err)
		return
	}
	c.MutexIn.Lock()
	defer c.MutexIn.Unlock()
	c.InputBuffer.Write(data[0:n])
}

func (c *Connection) HandleReadBufferFromConn() {
	for {
		if c.NeedClose.Load() {
			return
		}
		c.readBufferFromConn()
	}
}

func (c *Connection) HandleReadMsgFromBuff() {
	for {
		if c.NeedClose.Load() {
			return
		}
		c.readMsgFromBuff()
	}
}
