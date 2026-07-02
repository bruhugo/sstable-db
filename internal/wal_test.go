package db

import (
	"io"
	"sync"
	"testing"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

func createWalTest(t *testing.T) *WAL {
	dir := t.TempDir()
	wal := NewWAL()
	wal.Open(dir)
	return wal
}

func TestAppendWAL(t *testing.T) {
	wal := createWalTest(t)
	defer wal.Close()
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending record to WAL: %v", record)
		}
	}
}

func TestClearWAL(t *testing.T) {
	wal := createWalTest(t)
	defer wal.Close()
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending to wal: %s", err.Error())
		}
	}

	if err := wal.Clear(); err != nil {
		t.Errorf("error while clearing WAL: %s", err.Error())
	}
	_, err := wal.file.Read(make([]byte, 1))
	if err == nil {
		t.Error("clearing WAL expected EOF, but go nothing")
	}

	if err != io.EOF {
		t.Errorf("unexpected error while reading cleared WAL: %s", err.Error())
	}
}

func TestRecoverWAL(t *testing.T) {
	wal := createWalTest(t)
	defer wal.Close()
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending to wal: %s", err.Error())
		}
	}

	recoverTestWal(t, tb, wal)
}

func TestAppendConcurrentWAL(t *testing.T) {
	wal := createWalTest(t)
	defer wal.Close()
	tb := newRecordList()

	var wg sync.WaitGroup
	wg.Add(len(tb))
	for _, record := range tb {
		go func() {
			if err := wal.Append(record); err != nil {
				t.Error("error appending to WAL")
			}
			wg.Done()
		}()
	}
	wg.Wait()

	c := make(chan *pb.Record)
	go wal.Recover(c)
	read := 0
	for {
		_, ok := <-c
		if !ok {
			break
		}
		read++
	}

	if read != len(tb) {
		t.Errorf("expected to recover %d entries, but got %d", len(tb), read)
	}
}

func recoverTestWal(t *testing.T, tb []*pb.Record, wal *WAL) {
	c := make(chan *pb.Record)
	go wal.Recover(c)
	read := 0
	for {
		record, ok := <-c
		if !ok {
			break
		}

		if !proto.Equal(record, tb[read]) {
			t.Errorf("expected recovered entry %+v, but got %+v", tb[read], record)
		}

		read++
	}

	if read != len(tb) {
		t.Errorf("expected to recover %d entries, but got %d", len(tb), read)
	}
}

func newRecordList() []*pb.Record {
	return []*pb.Record{
		{Key: "key1", Value: "value1", SequenceNumber: 1},
		{Key: "key2", Value: "value2", SequenceNumber: 2},
		{Key: "key3", Value: "value3", SequenceNumber: 3},
		{Key: "key4", Value: "value4", SequenceNumber: 4},
		{Key: "key5", Value: "value5", SequenceNumber: 5},
		{Key: "key6", Value: "value6", SequenceNumber: 6},
	}
}

func BenchmarkAppendWAL(b *testing.B) {
	b.ReportAllocs()
	wal := NewWAL()
	wal.Open(b.TempDir())
	record := &pb.Record{Key: "key", Value: "value"}
	b.ResetTimer()

	for b.Loop() {
		wal.Append(record)
	}
}
