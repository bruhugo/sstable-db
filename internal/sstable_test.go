package db

import (
	"testing"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

type manifestStub struct {
}

func (m manifestStub) Recover() (RecoverData, error) {
	return RecoverData{}, nil
}
func (m manifestStub) AddLatestSequenceNumber(sequenceNumber uint64) error {
	return nil
}
func (m manifestStub) AddSSTablePath(sstable string) error {
	return nil
}
func (m manifestStub) RemoveSSTable(sstable string) error {
	return nil
}

func TestCreateSSTableNormal(t *testing.T) {
	records := newRecordList()

	dir := t.TempDir()
	sstables := NewSSTables(dir, manifestStub{})

	err := sstables.CreateSSTable(records)
	if err != nil {
		t.Errorf("unexpected error creating sstable: %s", err.Error())
	}

	found, ok := sstables.Search(records[0].Key)
	if !ok {
		t.Error("expected to find record, but none was found")
	}

	if found != records[0].Value {
		t.Error("found value does not match inserted one")
	}

	if len(sstables.tables) != 1 {
		t.Errorf("expected 1 table in sstable structure but found %d", len(sstables.tables))
	}
}

func TestDeletedEntry(t *testing.T) {
	records := newRecordList()

	dir := t.TempDir()
	sstables := NewSSTables(dir, manifestStub{})

	err := sstables.CreateSSTable(records)
	if err != nil {
		t.Errorf("unexpected error creating sstable: %s", err.Error())
	}

	records = append(records, &pb.Record{Key: records[0].Key, RecordType: pb.RecordType_RECORD_TYPE_DELETE})

	sstables.CreateSSTable(records)
	_, ok := sstables.Search(records[0].Key)
	if ok {
		t.Errorf("expected to not find removed entry, but it was found")
	}
}

// sstables must read the most recent entry of a given key,
// so a tombstone in a newer sstable should override any
// entries in the previously created sstables
func TestDeletedEntryInNewTable(t *testing.T) {
	records := newRecordList()

	dir := t.TempDir()
	sstables := NewSSTables(dir, manifestStub{})

	err := sstables.CreateSSTable(records)
	if err != nil {
		t.Errorf("unexpected error creating sstable: %s", err.Error())
	}

	err = sstables.CreateSSTable([]*pb.Record{{Key: records[0].Key, RecordType: pb.RecordType_RECORD_TYPE_DELETE}})
	if err != nil {
		t.Errorf("unexpected error creating sstable: %s", err.Error())
	}

	_, ok := sstables.Search(records[0].Key)
	if ok {
		t.Errorf("expected to not find removed entry, but it was found")
	}
}

func TestSSTableIterator(t *testing.T) {
	records := newRecordList()

	dir := t.TempDir()
	sstables := NewSSTables(dir, manifestStub{})
	if err := sstables.CreateSSTable(records); err != nil {
		t.Errorf("unexpected error creating sstable: %s", err)
	}

	it := sstables.tables[0].it
	for i := range records {
		if !it.Next() {
			t.Errorf("expected more records in iterator")
		}
		if !proto.Equal(it.record.Record, records[i]) {
			t.Error("unexpected value found in iterator")
		}
	}

	if it.Next() {
		t.Errorf("iterator found more records than it should")
	}
}

func TestRecoverSSTables(t *testing.T) {
	records := newRecordList()

	dir := t.TempDir()
	sstables := NewSSTables(dir, manifestStub{})
	if err := sstables.CreateSSTable(records); err != nil {
		t.Errorf("unexpected error creating sstable: %s", err)
	}
	name1 := sstables.tables[0].filename

	sstables.tables = sstables.tables[:0]
	sstables.tableSequenceNumber = 0

	err := sstables.RecoverSSTables(name1)
	if err != nil {
		t.Errorf("error recovering sstables: %s", err.Error())
	}

	if sstables.tableSequenceNumber != 1 {
		t.Errorf("expected sequence number 1, got %d", sstables.tableSequenceNumber)
	}

	found, ok := sstables.Search(records[0].Key)
	if !ok {
		t.Errorf("expected to find value, but none was found")
	}
	if found != records[0].Value {
		t.Errorf("expected to find value %s but found %s", records[0].Value, found)
	}
}

func TestSSTableMerge(t *testing.T) {
	records := newRecordList()
	dir := t.TempDir()

	sst := NewSSTables(dir, manifestStub{})
	if err := sst.CreateSSTable(records[:3]); err != nil {
		t.Errorf("unexpected error creating sstable: %s", err)
	}
	if err := sst.CreateSSTable(records[3:6]); err != nil {
		t.Errorf("unexpected error creating sstable: %s", err)
	}

	err := sst.Merge()
	if err != nil {
		t.Errorf("unexptected error merging sstables")
	}

	if len(sst.tables) != 1 {
		t.Errorf("unexpected table number")
	}

	found, ok := sst.Search(records[0].Key)
	if !ok {
		t.Errorf("expected to find value, but none was found")
	}
	if found != records[0].Value {
		t.Errorf("expected to find value %s but found %s", records[0].Value, found)
	}

	found, ok = sst.Search(records[5].Key)
	if !ok {
		t.Errorf("expected to find value, but none was found")
	}
	if found != records[5].Value {
		t.Errorf("expected to find value %s but found %s", records[5].Value, found)
	}
}

func BenchmarkSSTableCreate(b *testing.B) {
	b.ReportAllocs()
	sstable := NewSSTables(b.TempDir(), manifestStub{})
	records := make([]*pb.Record, 0)
	for range 1_000 {
		records = append(records, &pb.Record{Key: "key", Value: "value"})
	}
	b.ResetTimer()

	for b.Loop() {
		sstable.CreateSSTable(records)
	}
}
