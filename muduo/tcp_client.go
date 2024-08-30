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

func NewClient(ip string, port int, codec Codec) (*Client, error) {
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	conn, err := Connect(addr)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	c := &Client{
		Conn: Connection{
			Conn:       conn,
			OutMsgList: make(chan proto.Message, 100),
			InMsgList:  make(chan proto.Message, 100),
			Codec:      codec,
			Addr:       addr,
		},
	}

	go c.Conn.HandleWriteMsgToBuffer()
	go c.Conn.HandleReadBufferFromConn()
	return c, nil
}

func (c *Client) Close() error {
	return c.Conn.Conn.Close()
}

func (c *Client) Send(m proto.Message) {
	c.Conn.OutMsgList <- m
}
