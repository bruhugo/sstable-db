package protobuf_sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

type WAL struct {
	file *os.File
	mu   sync.RWMutex
}

func NewWAL() *WAL {
	return &WAL{}
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
	return nil
}

func (w *WAL) Append(record *pb.Record) error {
	crc := computeChecksum(record)
	walRecord := pb.WalRecord{
		Record:   record,
		Checksum: crc,
	}

	buffer := bufio.NewWriter(w.file)

	d, err := proto.Marshal(&walRecord)
	if err != nil {
		return fmt.Errorf("error marshaling protobuf WAL entry message: %w", err)
	}

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(d)))
	_, err = buffer.Write(b)
	if err != nil {
		return fmt.Errorf("error writing to WAL file: %w", err)
	}

	_, err = buffer.Write(d)
	if err != nil {
		return fmt.Errorf("error writing to WAL file: %w", err)
	}

	if err := buffer.Flush(); err != nil {
		return fmt.Errorf("error flushing buffer to WAL file: %w", err)
	}

	return nil
}

func (w *WAL) recover(c chan MetaRecord) {
	defer close(c)
	w.file.Seek(0, io.SeekStart)
	for {
		offset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}

		size, err := parseuint32(w.file)
		if errors.Is(err, io.EOF) {
			break
		}

		data := make([]byte, size)
		_, err = w.file.Read(data)
		if err != nil {
			w.file.Truncate(offset)
			break
		}

		var record pb.WalRecord
		err = proto.Unmarshal(data, &record)
		if err != nil {
			w.file.Truncate(offset)
			break
		}

		if record.Checksum != computeChecksum(record.Record) {
			w.file.Truncate(offset)
			break
		}

		c <- MetaRecord{
			record: record.Record,
			size:   size,
		}
	}
}
