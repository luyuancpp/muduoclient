package muduo

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"log"
)

type Codec interface {
	Encode(m *proto.Message) ([]byte, error)
	Decode(data []byte) (proto.Message, uint32, error)
}

//from muduo codec.cc
// struct ProtobufTransportFormat __attribute__ ((__packed__))
// {
//   int32_t  len;
//   int32_t  nameLen;
//   char     typeName[nameLen];
//   char     protobufData[len-nameLen-8];
//   int32_t  checkSum; // adler32 of nameLen, typeName and protobufData
// }

type TcpCodec struct {
	Codec
}

func (c *TcpCodec) Encode(m *proto.Message) ([]byte, error) {
	//learn from zinx
	d := GetDescriptor(m)

	msgNameLenData := make([]byte, 4)
	msgTypeName := d.Name() + " "
	binary.BigEndian.PutUint32(msgNameLenData, uint32(len(msgTypeName)))
	msgTypeNameData := []byte(msgTypeName)

	msgBodyData, err := proto.Marshal(*m)
	if err != nil {
		return []byte{}, err
	}

	dataMSG := make([]byte, 0)

	dataMSG = append(dataMSG, msgNameLenData...)
	dataMSG = append(dataMSG, msgTypeNameData...)
	dataMSG = append(dataMSG, msgBodyData...)

	checkSum := adler32.Checksum(dataMSG)
	checkSumData := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSumData, checkSum)
	dataMSG = append(dataMSG, checkSumData...)

	lenMSG := len(dataMSG)
	lenData := make([]byte, 4)
	binary.BigEndian.PutUint32(lenData, uint32(lenMSG))
	data := make([]byte, 0)
	data = append(data, lenData...)
	data = append(data, dataMSG...)
	return data, nil
}

func (c *TcpCodec) Decode(data []byte) (proto.Message, uint32, error) {
	//learn from zinx
	if len(data) < 8 {
		return nil, 0, nil
	}
	length := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < length {
		return nil, 0, nil
	}

	msgNameLen := binary.BigEndian.Uint32(data[4:8])
	dataIndex := msgNameLen + 8
	msgName := protoreflect.FullName(string(data[8 : dataIndex-1]))
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgName)
	if err != nil {
		log.Println(err)
		return nil, length, err
	}
	msg := proto.MessageV1(msgType.New())
	err = proto.Unmarshal(data[dataIndex:length], msg)
	if err != nil {
		return nil, length, err
	}

	checkSum := adler32.Checksum(data[4:length])
	checkSumData := binary.BigEndian.Uint32(data[length : length+4])
	if checkSum != checkSumData {
		log.Println("checksum")
	}
	length += 4
	return msg, length, nil
}

// wire format
//
// # Field     Length  Content
//
// size      4-byte  N+8
// "RPC0"    4-byte
// payload   N-byte
// checksum  4-byte  adler32 of "RPC0"+payload
type RpcCodec struct {
	Codec
	RpcMsgType proto.Message
}

func (c *RpcCodec) Encode(m *proto.Message) ([]byte, error) {
	//learn from zinx

	tagData := []byte("RPC0")

	msgBodyData, err := proto.Marshal(*m)
	if err != nil {
		return []byte{}, err
	}

	dataMsg := make([]byte, 0)

	dataMsg = append(dataMsg, tagData...)
	dataMsg = append(dataMsg, msgBodyData...)

	checkSum := adler32.Checksum(dataMsg)
	checkSumData := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSumData, checkSum)
	dataMsg = append(dataMsg, checkSumData...)

	lenMsg := len(dataMsg)
	lenData := make([]byte, 4)
	binary.BigEndian.PutUint32(lenData, uint32(lenMsg))
	data := make([]byte, 0)
	data = append(data, lenData...)
	data = append(data, dataMsg...)
	return data, nil
}

func (c *RpcCodec) Decode(data []byte) (proto.Message, uint32, error) {
	//learn from zinx
	if len(data) < 8 {
		return nil, 0, nil
	}
	length := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < length {
		return nil, 0, nil
	}

	msgName := protoreflect.FullName(GetDescriptor(&c.RpcMsgType).FullName())
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgName)
	if err != nil {
		log.Println(err)
		return nil, length, err
	}
	msg := proto.MessageV1(msgType.New())
	err = proto.Unmarshal(data[8:length], msg)
	if err != nil {
		return nil, length, err
	}

	checkSum := adler32.Checksum(data[4:length])
	checkSumData := binary.BigEndian.Uint32(data[length : length+4])
	if checkSum != checkSumData {
		log.Println("checksum")
	}
	length += 4
	return msg, length, nil
}
