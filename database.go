package protobuf_sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type Database struct {
	wal  *WAL
	memt *Memtable
}

type MetaRecord struct {
	size   uint32
	record *pb.Record
}

func NewDatabase() (*Database, error) {
	wal, err := NewWAL("wal")
	if err != nil {
		return nil, fmt.Errorf("error creating WAL: %w", err)
	}

	database := &Database{
		wal:  wal,
		memt: NewMemtable(),
	}

	c := make(chan MetaRecord)
	go wal.recover(c)

	for {
		record, ok := <-c
		if !ok {
			break
		}

		database.memt.add(record)
	}

	return database, nil
}

func (d *Database) append(key, value string) error {
	return nil
}

func (d *Database) get(key string) (string, error) {
	return "", nil
}

func (d *Database) delete(key string) error {
	return nil
}

func computeChecksum(a any) uint32 {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, a)

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return h.Sum32()
}
