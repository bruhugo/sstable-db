package protobuf_sstable

import (
	"context"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type Memtable struct {
	tree     *rbt.Tree
	size     uint64
	treshold uint64
	sstables SSTables
}

func NewMemtable(treshold uint64) *Memtable {
	return &Memtable{
		tree:     rbt.NewWithStringComparator(),
		treshold: treshold,
	}
}

func (mem *Memtable) add(mr MetaRecord) {
	mem.tree.Put(mr.record.Key, mr)
	mem.size += uint64(mr.size)
	// TODO: create sstable
}

func (mem *Memtable) get(key string) MetaRecord {
	record, _ := mem.tree.Get(key)
	return record.(MetaRecord)
}

// merge sstables async
func (mem *Memtable) mergeAsync(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: handle error somehow
			mem.sstables.merge()
			return
		}
	}
}
