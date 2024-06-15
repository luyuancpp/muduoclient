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
	Conn Connection
}

func NewClient(ip string, port int) (*Client, error) {
	conn, err := net.Dial("tcp", ip+":"+strconv.Itoa(port))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	c := &Client{
		Conn: Connection{conn: conn, M: make(chan proto.Message, 20)},
	}

	go c.Conn.HandleWriteMsgToBuffer()
	go c.Conn.HandleWriteBufferToConn()
	return c, nil
}

func (c *Client) Close() error {
	return c.Conn.conn.Close()
}

func (c *Client) Send(m proto.Message) {
	c.Conn.M <- m
}
