package muduo

import (
	"testing"
)

func TestClient(t *testing.T) {
	client, err := NewClient("127.0.0.1", 6000, &TcpCodec{})
	if err != nil {
		panic(err)
	}

	defer func(client *Client) {
		client.Close()
	}(client)
}
