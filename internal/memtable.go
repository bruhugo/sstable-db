package db

import (
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"google.golang.org/protobuf/proto"
)

type TreeHandle uint8

type Memtable struct {
	trees       [2]*rbt.Tree
	size        [2]uint64
	currentTree TreeHandle
	treshold    uint64
	mu          sync.RWMutex
}

func NewMemtable(treshold uint64) *Memtable {
	return &Memtable{
		trees: [2]*rbt.Tree{
			rbt.NewWithStringComparator(),
			rbt.NewWithStringComparator(),
		},
		currentTree: 0,
		treshold:    treshold,
	}
}

func (memt *Memtable) SetTreshold(t uint64) {
	memt.treshold = t
}

// NOT THREAD SAFE!!!
func (mem *Memtable) GetCurrentTree() *rbt.Tree {
	return mem.trees[mem.currentTree]
}

// NOT THREAD SAFE!!!
func (mem *Memtable) GetIdleTree() *rbt.Tree {
	return mem.trees[1-mem.currentTree]
}

func (mem *Memtable) Search(key string) (*pb.Record, bool) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	v, ok := mem.GetCurrentTree().Get(key)
	if !ok {
		v, ok = mem.GetIdleTree().Get(key)
		if !ok {
			return nil, false
		}
	}

	record, ok := v.(*pb.Record)
	if !ok {
		return nil, false
	}

	return record, true
}

// adds the metarecord to the memtable and return
// if a the threshold was met or not
func (mem *Memtable) Add(record *pb.Record) bool {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.GetCurrentTree().Put(record.Key, record)
	mem.size[mem.currentTree] += uint64(proto.Size(record))
	return mem.size[mem.currentTree] >= mem.treshold
}

// removes the record from both trees
func (mem *Memtable) Remove(key string) {
	record := &pb.Record{
		RecordType: pb.RecordType_RECORD_TYPE_DELETE,
		Key:        key,
	}
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.GetCurrentTree().Put(key, record)
}

// returns the list of metarecords and a handle used to clear the tree
// when you are done flushing to disk
func (mem *Memtable) GetValuesAndSwitch() ([]*pb.Record, TreeHandle) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	records := make([]*pb.Record, 0)

	for _, r := range mem.GetCurrentTree().Values() {
		mt, ok := r.(*pb.Record)
		if !ok {
			panic("value stored in rbt is not a record")
		}

		records = append(records, mt)
	}

	oldHandle := mem.currentTree
	mem.currentTree = 1 - oldHandle

	return records, oldHandle
}

func (mem *Memtable) ClearTree(handle TreeHandle) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.size[handle] = 0
	mem.trees[handle].Clear()
}

func (mem *Memtable) Get(key string) (*pb.Record, bool) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	record, ok := mem.GetCurrentTree().Get(key)
	if !ok {
		return nil, false
	}
	return record.(*pb.Record), true
}
