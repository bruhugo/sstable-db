package db

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

type RecordSize uint32

type SSTableIterator struct {
	file    *os.File
	record  *pb.SSTableRecord
	data    []byte
	key     string
	started bool
}

func (it *SSTableIterator) Next() bool {
	if !it.started {
		it.started = true
		it.file.Seek(0, io.SeekStart)
	}
	var size RecordSize
	err := binary.Read(it.file, binary.LittleEndian, &size)
	if err != nil {
		return false
	}

	b := make([]byte, size)
	if _, err := it.file.Read(b); err != nil {
		// TODO: maybe handle the error here
		return false
	}

	record := &pb.SSTableRecord{}
	err = proto.Unmarshal(b, record)
	if err != nil {
		return false
	}

	it.data = b
	it.record = record
	it.key = record.Record.Key

	return true
}

type IteratorHeap []*SSTableIterator

func (h IteratorHeap) Len() int { return len(h) }
func (h IteratorHeap) Less(i, j int) bool {
	key0 := h[i].key
	key1 := h[j].key

	return strings.Compare(key0, key1) == -1

}
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IteratorHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*SSTableIterator))
}

func (h *IteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type SSTable struct {
	it       *SSTableIterator
	filename string
	valid    bool
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

	if err := sst.manifest.LatestSequenceNumber(latestSequenceNumber); err != nil {
		return err
	}

	if err := sst.manifest.AddSSTable(filename); err != nil {
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
		},
	}

	for _, table := range oldTable {
		sst.manifest.RemoveSSTable(table.filename)
		os.Remove(table.filename)
	}

	os.Rename(file.Name(), newName)
	sst.manifest.AddSSTable(newName)

	return nil
}

func (sst *SSTables) Search(key string) (string, bool) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	for _, table := range sst.tables {
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

		return record.Record.Value, true
	}

	return "", false
}
