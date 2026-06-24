package protobuf_sstable

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

type WAL struct {
	file *os.File
	mu   sync.RWMutex
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

	return nil
}

func (w *WAL) recover(c chan *pb.Record) {
	defer close(c)
	w.file.Seek(0, io.SeekStart)
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
