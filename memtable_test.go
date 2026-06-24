package protobuf_sstable

import (
	"sync"
	"testing"

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
	value, ok := mem.Search(expected.Key)
	if !ok {
		t.Errorf("expected to find record with key %s in memtable, but didn't", expected.Key)
	}
	if value != expected.Value {
		t.Errorf("expected value %s, but got %s", expected.Value, value)
	}

	// not contain
	notContainKey := "not contain"
	value, ok = mem.Search(notContainKey)
	if ok {
		t.Errorf("expected to not find record with key %s in memtable, but didn't", notContainKey)
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
	value, ok := mem.Search(expected.Key)
	if !ok {
		t.Errorf("expected to find record with key %s in memtable, but didn't", expected.Key)
	}
	if value != expected.Value {
		t.Errorf("expected value %s, but got %s", expected.Value, value)
	}

	notContainKey := "not contain"
	value, ok = mem.Search(notContainKey)
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
	newValue, ok := mem.Search(newRecord.Key)
	if !ok {
		t.Error("expected to find new record in new tree, but didn't")
	}
	if newValue != newRecord.Value {
		t.Errorf("found %s but expected %s", newValue, newRecord.Value)
	}

	// search value stored in old tree
	oldRecord := tb[0]
	oldValue, ok := mem.Search(oldRecord.Key)
	if !ok {
		t.Error("expected to find old record in old tree, but didn't")
	}
	if oldValue != oldRecord.Value {
		t.Errorf("found %s but expected %s", oldValue, oldRecord.Value)
	}
}
