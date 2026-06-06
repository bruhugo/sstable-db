package protobuf_sstable

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

type SSTableIterator struct {
	file   *os.File
	record *pb.SSTableRecord
	data   []byte
	key    []byte
}

func (it *SSTableIterator) Next() bool {
	var size uint32
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

	return bytes.Compare(key0, key1) == -1

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
}

type SSTables struct {
	tables []SSTable
	mu     sync.Mutex
	dir    string
}

func createSSTableName(cur uint64) string {
	return fmt.Sprintf("%04d.sst", cur)
}

// merge picks the last two tables, merge them and
// add back to the list untill there is only one
func (sst *SSTables) merge() error {

	var h *IteratorHeap

	for _, table := range sst.tables {
		it := table.it
		if it.Next() {
			*h = append(*h, table.it)
		}
	}

	heap.Init(h)

	file, err := os.OpenFile("compressed.sst", os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	var lastkey []byte
	for h.Len() > 0 {
		it := heap.Pop(h).(*SSTableIterator)

		if bytes.Compare(lastkey, it.key) != 0 {
			if it.record.Record.RecordType != pb.RecordType_RECORD_TYPE_DELETE {
				if _, err := file.Write(it.data); err != nil {
					return err
				}
			}
			lastkey = it.key
		}

		if it.Next() {
			heap.Push(h, it)
		}
	}

	for _, table := range sst.tables {
		os.Remove(table.filename)
	}

	os.Rename(file.Name(), createSSTableName(0))
	sst.tables = []SSTable{
		{
			it:       &SSTableIterator{file: file},
			filename: file.Name(),
		},
	}

	return nil
}
