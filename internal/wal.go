package db

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type WALTimestamps struct {
	LastWritten   time.Time
	LastRecovered time.Time
	LastTruncated time.Time
}

type WALStat struct {
	Size    uint64
	Entries uint64
	WALTimestamps
}

type WAL struct {
	file    *os.File
	mu      sync.RWMutex
	entries uint64
	WALTimestamps
}

func NewWAL() *WAL {
	return &WAL{}
}

func NewWALRecord(record *pb.Record) *pb.WalRecord {
	crc := computeChecksum(record)
	return &pb.WalRecord{
		Record:   record,
		Checksum: crc,
	}
}

func (w *WAL) Open(dir string) error {
	file, err := os.OpenFile(fmt.Sprintf("%s/wal", dir), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("error opening/creating WAL file: %w", err)
	}
	w.file = file
	return nil
}

func (w *WAL) Close() {
	w.file.Close()
}

func (w *WAL) Clear() error {
	err := w.file.Truncate(0)
	if err != nil {
		return fmt.Errorf("error truncating wal file: %w", err)
	}
	_, err = w.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking to start of wal file: %w", err)
	}
	w.LastTruncated = time.Now()
	w.entries = 0
	return nil
}

func (w *WAL) Append(record *pb.Record) error {
	buffer := bufio.NewWriter(w.file)

	walRecord := NewWALRecord(record)
	if _, err := serializeWALRecord(walRecord, buffer); err != nil {
		return err
	}

	if err := buffer.Flush(); err != nil {
		return fmt.Errorf("error flushing buffer to WAL file: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("error flushing WAL file to disk")
	}

	w.LastWritten = time.Now()
	w.entries++

	return nil
}

func (w *WAL) Recover(c chan *pb.Record) {
	defer close(c)
	w.file.Seek(0, io.SeekStart)
	w.LastRecovered = time.Now()
	for {
		offset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}

		record, err := parseWALRecord(w.file)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				w.file.Truncate(offset)
			}
			break
		}

		c <- record.Record
	}
}

func (wal *WAL) Stat() (WALStat, error) {
	stat, err := wal.file.Stat()
	if err != nil {
		return WALStat{}, fmt.Errorf("error getting wal file stats")
	}

	size := uint64(stat.Size())

	return WALStat{
		WALTimestamps: wal.WALTimestamps,
		Entries:       wal.entries,
		Size:          size,
	}, nil
}
