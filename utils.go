package protobuf_sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

// serializes the keyoffset table AND its length as uint64 right after
func serializeKeyoffset(ko *pb.SSTableKeyPair, f io.Writer) (int, error) {
	kod, err := proto.Marshal(ko)
	if err != nil {
		// TODO: handle
		panic(err)
	}

	kodw, err := f.Write(kod)
	if err != nil {
		return kodw, err
	}

	lenw, err := f.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(kod))))
	if err != nil {
		return lenw + kodw, err
	}

	return kodw + lenw, nil
}

func serializeSSTableRecord(record MetaRecord, f io.Writer) (int, error) {
	sstrecord := &pb.SSTableRecord{
		Record:   record.record,
		Checksum: computeChecksum(record.record),
	}

	data, err := proto.Marshal(sstrecord)
	if err != nil {
		// TODO: handle error
		panic(err.Error())
	}

	l := uint32(len(data))
	headerw, err := f.Write(binary.LittleEndian.AppendUint32(nil, l))
	if err != nil {
		return 0, err
	}
	dataw, err := f.Write(data)
	if err != nil {
		return 0, err
	}

	return headerw + dataw, nil
}

func serializeUint32(n uint32, f *os.File) (int, error) {
	return f.Write(binary.LittleEndian.AppendUint32(nil, n))
}

func parseSSTableRecord(f *os.File, size uint32) (*pb.SSTableRecord, error) {
	buffer := make([]byte, size)
	if _, err := f.Read(buffer); err != nil {
		return nil, fmt.Errorf("error reading from sstable file")
	}
	record := &pb.SSTableRecord{}
	err := proto.Unmarshal(buffer, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func parseuint64(f *os.File) (uint64, error) {
	b := make([]byte, 8)
	if _, err := f.Read(b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func parseuint32(f *os.File) (uint32, error) {
	b := make([]byte, 4)
	if _, err := f.Read(b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func parseKeyoffset(f *os.File, size uint64) (*pb.SSTableKeyPair, error) {
	b := make([]byte, size)
	_, err := f.Read(b)
	if err != nil {
		return nil, err
	}

	m := &pb.SSTableKeyPair{}
	err = proto.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
