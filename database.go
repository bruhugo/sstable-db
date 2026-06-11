package protobuf_sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"time"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type Database struct {
	wal      *WAL
	memt     *Memtable
	sstables *SSTables
}

// MetaRecord is what is used between WalRecords and
// SSTableRecords in the program
type MetaRecord struct {
	size   uint32
	record *pb.Record
}

func NewMetaRecord(key, value string) *MetaRecord {
	return &MetaRecord{
		record: &pb.Record{
			Key:   key,
			Value: value,
		},
	}
}

func NewDatabase() (*Database, error) {
	wal, err := NewWAL("wal")
	if err != nil {
		return nil, fmt.Errorf("error creating WAL: %w", err)
	}

	database := &Database{
		wal:  wal,
		memt: NewMemtable(4000),
	}

	c := make(chan MetaRecord)
	go wal.recover(c)

	for {
		record, ok := <-c
		if !ok {
			break
		}

		database.memt.Add(record)
	}

	return database, nil
}

func (d *Database) Append(key, value string) error {
	mr := NewMetaRecord(key, value)
	if err := d.wal.Append(mr.record); err != nil {
		// TODO: handle error
		panic(err)
	}

	// memtable is full
	if d.memt.Add(*mr) {
		err := d.sstables.CreateSSTable(d.memt.GetAllContentsAndClear())
		if err != nil {
			panic(err)
		}
		err = d.wal.Clear()
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (d *Database) Get(key string) (string, error) {
	return "", nil
}

func (d *Database) Delete(key string) error {
	return nil
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

// merge sstables async
func (d *Database) MergeAsync(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: handle error somehow
			d.sstables.Merge()
			return
		}
	}
}
