package protobuf_sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"

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

func serializeUint32(n uint32, f io.Writer) (int, error) {
	return f.Write(binary.LittleEndian.AppendUint32(nil, n))
}

func serializeManifestRecord(record *pb.ManifestRecord, f io.Writer) (int, error) {
	d, err := proto.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("error marshaling manifest record: %w", err)
	}

	nw, err := serializeUint32(uint32(len(d)), f)
	if err != nil {
		return nw, fmt.Errorf("error writing to manifest file: %w", err)
	}

	rw, err := f.Write(d)
	if err != nil {
		return nw + rw, fmt.Errorf("error writing to manifest file: %w", err)
	}

	return nw + rw, nil
}

func parseSSTableRecord(f io.Reader, size uint32) (*pb.SSTableRecord, error) {
	buffer := make([]byte, size)
	if _, err := io.ReadFull(f, buffer); err != nil {
		return nil, fmt.Errorf("error reading from sstable file: %w", err)
	}
	record := &pb.SSTableRecord{}
	err := proto.Unmarshal(buffer, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func parseuint64(f io.Reader) (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(f, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func parseuint32(f io.Reader) (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(f, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func parseKeyoffset(f io.Reader, size uint64) (*pb.SSTableKeyPair, error) {
	b := make([]byte, size)
	_, err := io.ReadFull(f, b)
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

// ReadSeekTruncater represents an interface that satisfies io.ReadSeeker and has a Truncate method.
type ReadSeekTruncater interface {
	io.ReadSeeker
	Truncate(size int64) error
}

func parseManifestRecords(f ReadSeekTruncater) ([]*pb.ManifestRecord, error) {

	list := make([]*pb.ManifestRecord, 0)
	var offset int64 = 0
	f.Seek(0, io.SeekStart)
	size, err := parseuint32(f)
	for ; err == nil; size, err = parseuint32(f) {
		b := make([]byte, size)

		record := &pb.ManifestRecord{}
		err := proto.Unmarshal(b, record)
		if err != nil {
			break
		}

		list = append(list, record)
		newOffset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}

		offset = newOffset
	}

	if errors.Is(err, io.EOF) {
		return list, nil
	}
	f.Truncate(offset)
	return nil, err
}

func computeChecksum(r *pb.Record) uint32 {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, r.Key)
	binary.Write(&buf, binary.LittleEndian, r.Value)
	binary.Write(&buf, binary.LittleEndian, r.SequenceNumber)

	h := fnv.New32a()
	h.Write(buf.Bytes())

	return h.Sum32()
}
