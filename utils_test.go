package protobuf_sstable

import (
	"bytes"
	"testing"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

func TestParseSSTableRecord(t *testing.T) {
	original := &pb.SSTableRecord{
		Record: &pb.Record{
			Key:            "test-key",
			Value:          "test-value",
			SequenceNumber: 42,
			RecordType:     pb.RecordType_RECORD_TYPE_WRITE,
		},
		Checksum: 12345,
	}

	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal original record: %v", err)
	}

	reader := bytes.NewReader(data)
	parsed, err := parseSSTableRecord(reader, uint32(len(data)))
	if err != nil {
		t.Fatalf("unexpected error parsing sstable record: %v", err)
	}

	if !proto.Equal(original, parsed) {
		t.Errorf("parsed record doesn't match original.\nGot:  %+v\nWant: %+v", parsed, original)
	}

	readerShort := bytes.NewReader(data[:len(data)-1])
	_, err = parseSSTableRecord(readerShort, uint32(len(data)))
	if err == nil {
		t.Error("expected error due to short read, but got nil")
	}

	invalidData := []byte{0xff, 0xff, 0xff}
	readerInvalid := bytes.NewReader(invalidData)
	_, err = parseSSTableRecord(readerInvalid, uint32(len(invalidData)))
	if err == nil {
		t.Error("expected error due to invalid protobuf data, but got nil")
	}
}

func TestParseKeyoffset(t *testing.T) {
	// 1. Success case
	original := &pb.SSTableKeyPair{
		Keyoffset: map[string]uint64{
			"key-a": 100,
			"key-b": 200,
		},
	}

	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal original keyoffset: %v", err)
	}

	reader := bytes.NewReader(data)
	parsed, err := parseKeyoffset(reader, uint64(len(data)))
	if err != nil {
		t.Fatalf("unexpected error parsing keyoffset: %v", err)
	}

	if !proto.Equal(original, parsed) {
		t.Errorf("parsed keyoffset doesn't match original.\nGot:  %+v\nWant: %+v", parsed, original)
	}

	readerShort := bytes.NewReader(data[:len(data)-1])
	_, err = parseKeyoffset(readerShort, uint64(len(data)))
	if err == nil {
		t.Error("expected error due to short read, but got nil")
	}

	invalidData := []byte{0xff, 0xff, 0xff}
	readerInvalid := bytes.NewReader(invalidData)
	_, err = parseKeyoffset(readerInvalid, uint64(len(invalidData)))
	if err == nil {
		t.Error("expected error due to invalid protobuf data, but got nil")
	}
}
