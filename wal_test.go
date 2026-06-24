package protobuf_sstable

import (
	"io"
	"os"
	"sync"
	"testing"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

var wal WAL

func TestMain(m *testing.M) {
	file, err := os.CreateTemp("", "wal")
	if err != nil {
		panic(err)
	}
	defer os.Remove(file.Name())
	wal.file = file
	wal.Clear()
	m.Run()
}

func TestAppendWAL(t *testing.T) {
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending record to WAL: %v", record)
		}
	}
}

func TestClearWAL(t *testing.T) {
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending to wal: %s", err.Error())
		}
	}

	wal.Clear()
	_, err := wal.file.Read(make([]byte, 1))
	if err == nil {
		t.Error("clearing WAL expected EOF, but go nothing")
	}

	if err != io.EOF {
		t.Errorf("unexpected error while reading cleared WAL: %s", err.Error())
	}
}

func TestRecoverWAL(t *testing.T) {
	tb := newRecordList()

	for _, record := range tb {
		if err := wal.Append(record); err != nil {
			t.Errorf("error appending to wal: %s", err.Error())
		}
	}

	recoverTestWal(t, tb)
}

func TestAppendConcurrentWAL(t *testing.T) {
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
	go wal.recover(c)
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

func recoverTestWal(t *testing.T, tb []*pb.Record) {
	c := make(chan *pb.Record)
	go wal.recover(c)
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
