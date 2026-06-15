package protobuf_sstable

import (
	"sync"
	"unsafe"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
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

// NOT THREAD SAFE!!!
func (mem *Memtable) GetCurrentTree() *rbt.Tree {
	return mem.trees[mem.currentTree]
}

// NOT THREAD SAFE!!!
func (mem *Memtable) GetIdleTree() *rbt.Tree {
	return mem.trees[1-mem.currentTree]
}

func (mem *Memtable) Search(key string) (string, bool) {
	mem.mu.Lock()
	v, ok := mem.GetCurrentTree().Get(key)
	if !ok {
		// get the idle tree while in the lock
		idleTree := mem.GetIdleTree()
		mem.mu.Unlock()
		v, ok = idleTree.Get(key)
		if !ok {
			mem.mu.Unlock()
			return "", false
		}
	}

	metaRecord, ok := v.(MetaRecord)
	if !ok {
		return "", false
	}

	return metaRecord.record.Value, true
}

// adds the metarecord to the memtable and return
// if a the threshold was met or not
func (mem *Memtable) Add(mr MetaRecord) bool {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.GetCurrentTree().Put(mr.record.Key, mr)
	mem.size[mem.currentTree] += GetRecordSize(&mr)
	return mem.size[mem.currentTree] >= mem.treshold
}

// Estimates the record size. It's enough for the memtable,
// since it doesn't need to be precise
func GetRecordSize(r *MetaRecord) uint64 {
	return uint64(len(r.record.Key)) +
		uint64(len(r.record.Value)) +
		uint64(unsafe.Sizeof(r.record.SequenceNumber)) +
		uint64(unsafe.Sizeof(r.record.SequenceNumber))
}

// returns the list of metarecords and a handle used to clear the tree
// when you are done flushing to disk
func (mem *Memtable) GetValuesAndSwitch() ([]MetaRecord, TreeHandle) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	records := make([]MetaRecord, 0)

	for _, r := range mem.GetCurrentTree().Values() {
		mt, ok := r.(MetaRecord)
		if !ok {
			panic("value stored in rbt is not a metarecord")
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

func (mem *Memtable) Get(key string) (MetaRecord, bool) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	record, ok := mem.GetCurrentTree().Get(key)
	if !ok {
		return MetaRecord{}, false
	}
	return record.(MetaRecord), true
}
