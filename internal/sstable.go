package db

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type RecordSize uint32

type SSTableStat struct {
	Size      uint64
	Path      string
	Entries   uint32
	CreatedAt time.Time
}

type SSTablesStat struct {
	Tables      []SSTableStat
	TableNumber uint32
	TotalSize   uint64
	LastMerged  time.Time
}

type SSTable struct {
	it        *SSTableIterator
	filename  string
	valid     bool
	entries   uint32
	createdAt time.Time
}

func (s *SSTable) Stat() (SSTableStat, error) {
	stat, err := s.it.file.Stat()
	if err != nil {
		return SSTableStat{}, err
	}

	return SSTableStat{
		Entries:   s.entries,
		Size:      uint64(stat.Size()),
		CreatedAt: s.createdAt,
	}, nil
}

func newSSTable(file *os.File) *SSTable {
	return &SSTable{
		filename: file.Name(),
		it: &SSTableIterator{
			file: file,
		},
	}
}

type SSTables struct {
	tables              []*SSTable
	mu                  sync.RWMutex
	dir                 string
	tableSequenceNumber uint64
	manifest            Manifest
	lastMerged          time.Time
}

func NewSSTables(dir string, manifest Manifest) *SSTables {
	return &SSTables{
		dir:      dir,
		manifest: manifest,
	}
}

func (sst *SSTables) SetDir(dir string) {
	sst.dir = dir
}

func (sst *SSTables) CreateSSTable(records []*pb.Record) error {
	sst.mu.Lock()
	filename := sst.createSSTableName(sst.tableSequenceNumber)
	sst.tableSequenceNumber++
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err.Error())
	}

	t := newSSTable(file)
	t.entries = uint32(len(records))
	sst.tables = append(sst.tables, t)
	sst.mu.Unlock()

	ko := pb.SSTableKeyPair{
		Keyoffset: make(map[string]uint64),
	}

	buffer := bufio.NewWriter(file)
	var offset uint64 = 0
	var latestSequenceNumber uint64 = 0
	for _, record := range records {
		latestSequenceNumber = max(latestSequenceNumber, record.SequenceNumber)
		w, err := serializeSSTableRecord(record, buffer)
		if err != nil {
			return err
		}
		ko.Keyoffset[record.Key] = offset
		offset += uint64(w)
	}

	if _, err := serializeKeyoffset(&ko, buffer); err != nil {
		return err
	}

	if err := sst.manifest.AddLatestSequenceNumber(latestSequenceNumber); err != nil {
		return err
	}

	if err := sst.manifest.AddSSTablePath(filename); err != nil {
		return err
	}

	buffer.Flush()

	t.valid = true
	return nil
}

func (sst *SSTables) RecoverSSTables(sstables ...string) error {
	var i uint64 = 0
	defer func() {
		sst.tableSequenceNumber = i
	}()

	for _, tablename := range sstables {
		file, err := os.OpenFile(tablename, os.O_APPEND|os.O_RDWR, 0600)
		if err != nil {
			return fmt.Errorf("error opening recovered sstable: %w", err)
		}

		table := &SSTable{
			filename: tablename,
			it: &SSTableIterator{
				file: file,
			},
			valid: true,
		}

		sst.tables = append(sst.tables, table)
		i++
	}

	return nil
}

func (sst *SSTables) createSSTableName(cur uint64) string {
	return fmt.Sprintf("%s/%04d.sst", sst.dir, cur)
}

func (sst *SSTables) Merge() error {
	if len(sst.tables) <= 1 {
		return nil
	}

	h := make(IteratorHeap, 0)

	for _, table := range sst.tables {
		table.it.file.Seek(0, io.SeekStart)
		if table.valid && table.it.Next() {
			h = append(h, table.it)
		}
	}

	heap.Init(&h)

	file, err := os.OpenFile(sst.dir+"/compressed.sst", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	keyoffset := pb.SSTableKeyPair{
		Keyoffset: make(map[string]uint64),
	}

	var lastkey string
	var offset uint64 = 0
	for h.Len() > 0 {
		it := heap.Pop(&h).(*SSTableIterator)
		if lastkey == it.key {
			continue
		}

		lastkey = it.key
		if it.record.Record.RecordType == pb.RecordType_RECORD_TYPE_DELETE {
			continue
		}

		if _, err := serializeUint32(uint32(len(it.data)), file); err != nil {
			return err
		}

		if _, err := file.Write(it.data); err != nil {
			return err
		}

		keyoffset.Keyoffset[it.record.Record.Key] = offset
		offset += uint64(len(it.data) + 4)

		if it.Next() {
			heap.Push(&h, it)
		}
	}

	_, err = serializeKeyoffset(&keyoffset, file)
	if err != nil {
		return err
	}

	sst.mu.Lock()
	defer sst.mu.Unlock()

	oldTable := sst.tables
	newName := sst.createSSTableName(0)
	sst.tableSequenceNumber = 1
	sst.tables = []*SSTable{
		{
			it:       &SSTableIterator{file: file},
			filename: newName,
			valid:    true,
		},
	}

	sst.lastMerged = time.Now()

	for _, table := range oldTable {
		sst.manifest.RemoveSSTable(table.filename)
		err := os.Remove(table.filename)
		if err != nil {
			panic("error removing sstable file: " + err.Error())
		}
	}

	os.Rename(file.Name(), newName)
	sst.manifest.AddSSTablePath(newName)

	return nil
}

func (sst *SSTables) Search(key string) (string, bool) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	// always look in the most recent one
	cpy := slices.Clone(sst.tables)
	slices.Reverse(cpy)

	for _, table := range cpy {
		file := table.it.file

		file.Seek(-8, io.SeekEnd)

		mapSize, err := parseuint64(file)
		if err != nil {
			return "", false
		}

		noff := -int64(mapSize) - 8
		file.Seek(noff, io.SeekEnd)
		messagebuff, err := parseKeyoffset(file, mapSize)
		if err != nil {
			return "", false
		}

		found, ok := messagebuff.Keyoffset[key]
		if !ok {
			continue
		}

		file.Seek(int64(found), io.SeekStart)
		recordSize, err := parseuint32(file)
		if err != nil {
			return "", false
		}

		record, err := parseSSTableRecord(file, recordSize)
		if err != nil {
			return "", false
		}

		if record.Record.RecordType == pb.RecordType_RECORD_TYPE_DELETE {
			return "", false
		}

		return record.Record.Value, true
	}

	return "", false
}

func (sst *SSTables) Stat() (SSTablesStat, error) {
	stat := SSTablesStat{
		TableNumber: uint32(len(sst.tables)),
		LastMerged:  sst.lastMerged,
	}
	for _, t := range sst.tables {
		tstat, err := t.Stat()
		if err != nil {
			return SSTablesStat{}, fmt.Errorf("error getting stats for sstable: %w", err)
		}
		stat.TotalSize += tstat.Size
		stat.Tables = append(stat.Tables, tstat)
	}

	return stat, nil
}
