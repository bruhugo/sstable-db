package protobuf_sstable

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type Database struct {
	wal                 *WAL
	memt                *Memtable
	sstables            *SSTables
	manifest            Manifest
	entrySequenceNumber uint64
	mu                  sync.Mutex
	dir                 string
}

// MetaRecord is what is used between WalRecords and
// SSTableRecords in the program
type MetaRecord struct {
	size   uint32
	record *pb.Record
}

func NewMetaRecord(key, value string, sequenceNumber uint64) *MetaRecord {
	return &MetaRecord{
		record: &pb.Record{
			Key:            key,
			Value:          value,
			SequenceNumber: sequenceNumber,
			RecordType:     pb.RecordType_RECORD_TYPE_WRITE,
		},
	}
}

type DatabaseDecorator func(*Database)

func SetMemtableTreshold(t uint64) DatabaseDecorator {
	return func(d *Database) {
		d.memt.treshold = t
	}
}

func SetDirectory(path string) DatabaseDecorator {
	return func(d *Database) {
		d.sstables.dir = d.dir
	}
}

const DEFAULT_DB_DIR string = "db"

func NewDatabase(dbDecorators ...DatabaseDecorator) (*Database, error) {
	manifest := NewManifestImpl()
	wal := NewWAL()

	database := &Database{
		wal:      wal,
		memt:     NewMemtable(4000),
		sstables: NewSSTables(DEFAULT_DB_DIR, manifest),
		manifest: manifest,
		dir:      DEFAULT_DB_DIR,
	}

	for _, d := range dbDecorators {
		d(database)
	}

	if _, err := os.Stat(database.dir); err != nil {
		if err = os.MkdirAll(database.dir, 0755); err != nil {
			return nil, err
		}
	}

	if err := wal.Open(database.dir); err != nil {
		return nil, err
	}
	if err := manifest.Open(database.dir); err != nil {
		return nil, err
	}

	err := database.recover()
	if err != nil {
		return nil, fmt.Errorf("error while recovering state: %w", err)
	}

	go database.MergeAsync(context.Background())

	return database, nil
}

func (d *Database) recover() error {
	recoverData, err := d.manifest.Recover()
	if err != nil {
		return err
	}

	err = d.sstables.recoverSSTables(recoverData.sstables...)
	if err != nil {
		return err
	}

	d.entrySequenceNumber = recoverData.lastSequenceNumber

	c := make(chan *MetaRecord, 10)
	go d.wal.recover(c)

	for {
		record, ok := <-c
		if !ok {
			break
		}

		d.entrySequenceNumber = record.record.SequenceNumber
		d.memt.Add(record)
	}

	return nil
}

func (d *Database) Append(key, value string) error {
	// replace with atomic incrementer without locks
	d.mu.Lock()
	sequenceNumber := d.entrySequenceNumber
	d.entrySequenceNumber++
	d.mu.Unlock()

	mr := NewMetaRecord(key, value, sequenceNumber)
	if err := d.wal.Append(mr.record); err != nil {
		// TODO: handle error
		panic(err)
	}

	// memtable is full
	if d.memt.Add(mr) {
		values, handle := d.memt.GetValuesAndSwitch()
		err := d.sstables.CreateSSTable(values)
		if err != nil {
			panic(err)
		}
		err = d.wal.Clear()
		if err != nil {
			panic(err)
		}
		d.memt.ClearTree(handle)
	}

	return nil
}

func (d *Database) Get(key string) (string, error) {
	// TODO: finish implementing that, make it search in the sstables too
	metarecord, ok := d.memt.Get(key)
	if ok {
		return metarecord.record.Value, nil
	}

	value, ok := d.sstables.Search(key)
	if !ok {
		return "", nil
	}

	return value, nil
}

func (d *Database) Delete(key string) error {
	return nil
}

// merge sstables async
func (d *Database) MergeAsync(ctx context.Context) {
	// TODO: make it configurable
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
