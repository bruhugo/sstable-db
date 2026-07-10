package db

import (
	"encoding/binary"
	"io"
	"os"
	"strings"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

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
