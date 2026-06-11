package protobuf_sstable

import (
	"bufio"
	"bytes"
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
	file   *os.File
	record *pb.SSTableRecord
	data   []byte
	key    string
}

func (it *SSTableIterator) Next() bool {
	var size RecordSize
	err := binary.Read(it.file, binary.LittleEndian, &size)
	if err != nil {
		return false
	}

	b := make([]byte, size)

	var record *pb.SSTableRecord
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

// TODO: implement manifest
type Manifest struct {
	manifest pb.ManifestContent
	filepath string
	file     *os.File
	mu       sync.Mutex
}

type SSTables struct {
	tables         []*SSTable
	mu             sync.RWMutex
	manifest       Manifest
	dir            string
	sequenceNumber uint64
}

func (sst *SSTables) CreateSSTable(mrs []MetaRecord) error {
	// we lock here to conquer the sstable sequence number
	// once we have it, we can unlock and write to the
	// file

	sst.mu.Lock()
	filename := createSSTableName(sst.sequenceNumber)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		// TODO: handle it gracefully
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
	for _, mr := range mrs {
		data, err := serializeMetarecord(mr)
		if err != nil {
			// TODO: handle error
			panic(err.Error())
		}
		buffer.Write(data)
		offset += uint64(len(data) + 8)
		ko.Keyoffset[mr.record.Key] = offset
	}

	kod, err := proto.Marshal(&ko)
	if err != nil {
		// TODO: handle
		panic(err)
	}

	buffer.Write(kod)
	buffer.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(kod))))

	t.valid = true

	return nil
}

func serializeMetarecord(record MetaRecord) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))

	sstrecord := &pb.SSTableRecord{
		Record:   record.record,
		Checksum: computeChecksum(record.record),
	}

	data, err := proto.Marshal(sstrecord)
	if err != nil {
		// TODO: handle error
		panic(err.Error())
	}

	buffer.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(data))))
	buffer.Write(data)

	return buffer.Bytes(), nil
}

func createSSTableName(cur uint64) string {
	return fmt.Sprintf("%04d.sst", cur)
}

// merge picks the last two tables, merge them and
// add back to the list untill there is only one
func (sst *SSTables) Merge() error {

	h := make(IteratorHeap, 0)

	for _, table := range sst.tables {
		it := table.it
		if table.valid && it.Next() {
			h = append(h, table.it)
		}
	}

	heap.Init(&h)

	file, err := os.OpenFile("compressed.sst", os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	var lastkey string
	for h.Len() > 0 {
		it := heap.Pop(&h).(*SSTableIterator)

		if lastkey != it.key {
			if it.record.Record.RecordType != pb.RecordType_RECORD_TYPE_DELETE {
				if _, err := file.Write(it.data); err != nil {
					return err
				}
			}
			lastkey = it.key
		}

		if it.Next() {
			heap.Push(&h, it)
		}
	}

	sst.mu.Lock()
	defer sst.mu.Unlock()

	oldTable := sst.tables
	newName := createSSTableName(0)
	sst.tables = []*SSTable{
		{
			it:       &SSTableIterator{file: file},
			filename: newName,
		},
	}

	for _, table := range oldTable {
		os.Remove(table.filename)
	}

	os.Rename(file.Name(), newName)

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

		file.Seek(-int64(mapSize), io.SeekCurrent)
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

func parseSSTableRecord(f *os.File, size uint32) (*pb.SSTableRecord, error) {
	buffer := make([]byte, size)
	var record *pb.SSTableRecord
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

	var m *pb.SSTableKeyPair
	err = proto.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
