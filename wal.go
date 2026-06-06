package protobuf_sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
	"google.golang.org/protobuf/proto"
)

type WAL struct {
	file     *os.File
	filename string
	mu       sync.RWMutex
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("error to open/create WAL file: %w", err)
	}

	wal := &WAL{
		file:     file,
		filename: filename,
	}

	return wal, nil
}

func (w *WAL) close() {
	w.file.Close()
}

func (w *WAL) append(record *pb.Record) error {
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

	size := strconv.FormatInt(int64(len(d)), 10)
	_, err = buffer.Write([]byte(size))
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

		var size uint32
		err = binary.Read(w.file, binary.LittleEndian, &size)
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
