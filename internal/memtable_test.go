package db

import (
	"fmt"
	"sync"
	"testing"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

func TestAddAndSearchMemtable(t *testing.T) {
	mem := NewMemtable(100000)

	tb := newRecordList()

	for _, record := range tb {
		mem.Add(record)
	}

	// normal search
	expected := tb[0]
	record, ok := mem.Search(expected.Key)
	if !ok {
		t.Errorf("expected to find record with key %s in memtable, but didn't", expected.Key)
	}
	if record.Value != expected.Value {
		t.Errorf("expected value %s, but got %s", expected.Value, record.Value)
	}

	// not contain
	notContainKey := "not contain"
	_, ok = mem.Search(notContainKey)
	if ok {
		t.Errorf("expected to not find record with key %s in memtable, but didn't", notContainKey)
	}
}

func TestRemoveMemtable(t *testing.T) {
	mem := NewMemtable(100000)
	mem.Add(&pb.Record{Key: "key", Value: "value"})
	_, ok := mem.Search("key")
	if !ok {
		t.Error("expected to find added entry, but found nothing")
	}

	mem.Remove("key")

	found, ok := mem.Search("key")
	if !ok {
		t.Error("expected to find removed entry, but it was not found")
	}
	if found.RecordType != pb.RecordType_RECORD_TYPE_DELETE {
		t.Error("expected record to be a tombstone, but it is not")
	}
}

func TestAddConcurrentMemtable(t *testing.T) {
	mem := NewMemtable(100000)
	tb := newRecordList()

	var wg sync.WaitGroup
	wg.Add(len(tb))
	for _, record := range tb {
		go func() {
			mem.Add(record)
			wg.Done()
		}()
	}
	wg.Wait()

	expected := tb[0]
	record, ok := mem.Search(expected.Key)
	if !ok {
		t.Errorf("expected to find record with key %s in memtable, but didn't", expected.Key)
	}
	if record.Value != expected.Value {
		t.Errorf("expected value %s, but got %s", expected.Value, record.Value)
	}

	notContainKey := "not contain"
	_, ok = mem.Search(notContainKey)
	if ok {
		t.Errorf("expected to not find record with key %s in memtable, but didn't", notContainKey)
	}
}

func TestIfMemtableReturnsTrueWhenTreshold(t *testing.T) {
	mem := NewMemtable(1)
	isFull := mem.Add(NewRecord("key", "value", 1))
	if !isFull {
		t.Error("expecter full memtable, but it is not")
	}
}

func TestGetValuesAndSwitch(t *testing.T) {
	mem := NewMemtable(10000)
	tb := newRecordList()

	for _, record := range tb {
		mem.Add(record)
	}

	values, treeHandle := mem.GetValuesAndSwitch()
	if treeHandle == mem.currentTree {
		t.Error("tree handle should have switched")
	}

	for i, record := range values {
		if !proto.Equal(tb[i], record) {
			t.Errorf("expected %+v, got %+v", tb[i], record)
		}
	}

	newRecord := NewRecord("newkey", "newvalue", 12)
	mem.Add(newRecord)

	// search value stored in new tree
	foundRecord, ok := mem.Search(newRecord.Key)
	if !ok {
		t.Error("expected to find new record in new tree, but didn't")
	}
	if foundRecord.Value != newRecord.Value {
		t.Errorf("found %s but expected %s", foundRecord.Value, newRecord.Value)
	}

	// search value stored in old tree
	oldRecord, ok := mem.Search(tb[0].Key)
	if !ok {
		t.Error("expected to find old record in old tree, but didn't")
	}
	if oldRecord.Value != oldRecord.Value {
		t.Errorf("found %s but expected %s", oldRecord.Value, oldRecord.Value)
	}
}

func BenchmarkAddToMemtable(b *testing.B) {
	b.ReportAllocs()
	memtable := NewMemtable(200)
	b.ResetTimer()

	i := 0
	for b.Loop() {
		memtable.Add(&pb.Record{Key: fmt.Sprintf("%d", i), Value: "value1"})
		i++
	}
}

func BenchmarkAddToMemtableParallel(b *testing.B) {
	b.ReportAllocs()
	memtable := NewMemtable(200)
	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			memtable.Add(&pb.Record{Key: fmt.Sprintf("%d", i), Value: "value1"})
			i++
		}
	})
}
