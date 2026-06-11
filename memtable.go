package protobuf_sstable

import (
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type Memtable struct {
	tree     *rbt.Tree
	size     uint64
	treshold uint64
	mu       sync.RWMutex
}

func NewMemtable(treshold uint64) *Memtable {
	return &Memtable{
		tree:     rbt.NewWithStringComparator(),
		treshold: treshold,
	}
}

func (mem *Memtable) Search(key string) (string, bool) {
	v, ok := mem.tree.Get(key)
	if !ok {
		return "", false
	}

	metaRecord, ok := v.(MetaRecord)
	if !ok {
		return "", false
	}

	return metaRecord.record.Key, true
}

// adds the metarecord to the memtable and return
// if a the threshold was met or not
func (mem *Memtable) Add(mr MetaRecord) bool {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.tree.Put(mr.record.Key, mr)
	mem.size += uint64(mr.size)
	return mem.size >= mem.treshold
}

func (mem *Memtable) GetAllContentsAndClear() []MetaRecord {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	records := make([]MetaRecord, 0)

	for _, r := range mem.tree.Values() {
		mt, ok := r.(MetaRecord)
		if !ok {
			panic("value stored in rbt is not a metarecord")
		}

		records = append(records, mt)
	}

	mem.tree.Clear()

	return records
}

func (mem *Memtable) Get(key string) MetaRecord {
	record, _ := mem.tree.Get(key)
	return record.(MetaRecord)
}
