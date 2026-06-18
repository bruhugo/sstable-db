package protobuf_sstable

import (
	"fmt"
	"os"

	pb "github.com/bruhugo/protobuf_sstable/gen/go"
)

// This is responsible to keep track of the current state
// of the database. It records sstable files and sequence
// numebers used in the database.
type Manifest interface {
	Recover() (RecoverData, error)
	LatestSequenceNumber(sequenceNumber uint64) error
	AddSSTable(sstable string) error
	RemoveSSTable(sstable string) error
}

type ManifestImpl struct {
	file *os.File
}

type RecoverData struct {
	sstables           []string
	lastSequenceNumber uint64
}

func NewManifestImpl() *ManifestImpl {
	return &ManifestImpl{}
}

func (m *ManifestImpl) Open(dir string) error {
	file, err := os.OpenFile(dir+"/manifest", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("error opening manifest file: %w", err)
	}
	m.file = file
	return nil
}

func (m *ManifestImpl) Recover() (RecoverData, error) {
	records, err := parseManifestRecords(m.file)
	if err != nil {
		return RecoverData{}, fmt.Errorf("error parsing manifest records: %w", err)
	}

	set := make(map[string]struct{})

	recoverData := RecoverData{}
	for _, r := range records {
		switch r.Type {
		case pb.ManifestRecordType_LAST_SEQUENCE_NUMBER:
			recoverData.lastSequenceNumber = r.Sequence

		case pb.ManifestRecordType_ADD_SSTABLE:
			set[r.Filename] = struct{}{}

		case pb.ManifestRecordType_REMOVE_SSTABLE:
			delete(set, r.Filename)
		}
	}

	for k := range set {
		recoverData.sstables = append(recoverData.sstables, k)
	}

	return recoverData, nil
}

// NOT THREAD SAFE!!! USE YOUR OWN LOCK
func (m *ManifestImpl) LatestSequenceNumber(sequenceNumber uint64) error {
	record := &pb.ManifestRecord{
		Type:     pb.ManifestRecordType_LAST_SEQUENCE_NUMBER,
		Sequence: sequenceNumber,
	}

	_, err := serializeManifestRecord(record, m.file)
	if err != nil {
		return err
	}

	return nil
}

func (m *ManifestImpl) AddSSTable(sstable string) error {
	record := &pb.ManifestRecord{
		Type:     pb.ManifestRecordType_ADD_SSTABLE,
		Filename: sstable,
	}

	_, err := serializeManifestRecord(record, m.file)
	if err != nil {
		return err
	}

	return nil
}

func (m *ManifestImpl) RemoveSSTable(sstable string) error {
	record := &pb.ManifestRecord{
		Type:     pb.ManifestRecordType_REMOVE_SSTABLE,
		Filename: sstable,
	}

	_, err := serializeManifestRecord(record, m.file)
	if err != nil {
		return err
	}

	return nil
}
