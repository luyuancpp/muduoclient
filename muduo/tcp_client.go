package muduo

import (
	"github.com/golang/protobuf/proto"
	"log"
)

import (
	"net"
	"strconv"
)

type Client struct {
	Conn *Connection
}

func NewClient(ip string, port int, codec Codec) (*Client, error) {
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	conn, err := NewConnection(addr, codec)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	c := &Client{
		Conn: conn,
	}

	go c.Conn.HandleWriteMsgToBuffer()
	go c.Conn.HandleReadBufferFromConn()
	return c, nil
}

func (c *Client) Close() error {
	c.Conn.NeedClose.Store(true)
	close(c.Conn.OutMsgList)
	close(c.Conn.InMsgList)
	return c.Conn.Conn.Close()
}

func (c *Client) Send(m proto.Message) {
	c.Conn.OutMsgList <- m
}

func (c *Client) Recv() proto.Message {
	return <-c.Conn.InMsgList
}
