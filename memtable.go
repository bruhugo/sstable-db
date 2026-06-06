package protobuf_sstable

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type SSTable struct {
	filename string
}

type SSTables struct {
	tables []SSTable
	mu     sync.Mutex
	dir    string
}

func createSSTableName(cur uint64) string {
	return fmt.Sprintf("%010d.sst", cur)
}

func getSSTableNumber(name string) uint64 {
	before, _, ok := strings.Cut(name, ".")
	if !ok {
		panic("invalid sstable filename")
	}

	n, err := strconv.ParseInt(before, 10, 64)
	if err != nil {
		panic("invalid sstable name")
	}

	return uint64(n)
}

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

			return
		}
	}
}

// merge picks the last two tables, merge them and
// add back to the list untill there is only one
func (sst *SSTables) merge() error {
	if len(sst.tables) == 0 {
		return nil
	}
	for len(sst.tables) > 1 {
		temp, err := os.OpenFile("temp.sst", os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		l := len(sst.tables)
		table0 := sst.tables[l-1]
		table1 := sst.tables[l-2]

		file0, err := os.Open(table0.filename)
		if err != nil {
			return err
		}

		file1, err := os.Open(table1.filename)
		if err != nil {
			return err
		}

		for {

		}
	}

}
