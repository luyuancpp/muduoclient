package muduo

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"log"
)

//from muduo codec.cc
// struct ProtobufTransportFormat __attribute__ ((__packed__))
// {
//   int32_t  len;
//   int32_t  nameLen;
//   char     typeName[nameLen];
//   char     protobufData[len-nameLen-8];
//   int32_t  checkSum; // adler32 of nameLen, typeName and protobufData
// }

func Encode(m *proto.Message) ([]byte, error) {
	//learn from zinx
	d := GetDescriptor(m)

	pbNameLenData := make([]byte, 4)
	pbTypeName := d.Name() + " "
	binary.BigEndian.PutUint32(pbNameLenData, uint32(len(pbTypeName)))
	pbTypeNameData := []byte(pbTypeName)

	pbBodyData, err := proto.Marshal(*m)
	if err != nil {
		return []byte{}, err
	}

	dataPB := make([]byte, 0)

	dataPB = append(dataPB, pbNameLenData...)
	dataPB = append(dataPB, pbTypeNameData...)
	dataPB = append(dataPB, pbBodyData...)

	checkSum := adler32.Checksum(dataPB)
	checkSumData := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSumData, checkSum)
	dataPB = append(dataPB, checkSumData...)

	lenPB := len(dataPB)
	lenData := make([]byte, 4)
	binary.BigEndian.PutUint32(lenData, uint32(lenPB))
	data := make([]byte, 0)
	data = append(data, lenData...)
	data = append(data, dataPB...)
	return data, nil
}

func Decode(data []byte) (proto.Message, uint32, error) {
	//learn from zinx
	if len(data) < 8 {
		return nil, 0, nil
	}
	length := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < length {
		return nil, 0, nil
	}

	pbNameLen := binary.BigEndian.Uint32(data[4:8])
	dataIndex := pbNameLen + 8
	pbTypeName := string(data[8: dataIndex-1])
	msgName := protoreflect.FullName(pbTypeName)
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
	checkSumData := binary.BigEndian.Uint32(data[length : length+ 4])
	if checkSum != checkSumData {
		log.Println("checksum")
	}
	length += 4
	return msg, length, nil
}
