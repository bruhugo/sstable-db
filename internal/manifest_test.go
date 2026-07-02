package db

import (
	"fmt"
	"testing"
)

func TestAddLastestSequenceNumber(t *testing.T) {
	manifest := openTestManifest(t)

	addSequenceNumberAndTestManifest(t, manifest, []uint64{0, 20, 10, 9_999_999_999_999})
}

func TestAddSSTablePath(t *testing.T) {
	manifest := openTestManifest(t)

	addSStableAndTestManifest(t, manifest, []string{
		"/0000.sst",
		"/0001.sst",
		"/0002.sst",
		"/0003.sst",
		"/0004.sst",
	})
}

func TestRecover(t *testing.T) {
	manifest := openTestManifest(t)

	sequenceNumbers := []uint64{10, 11, 12}
	sstables := []string{"0001.sst", "0002.sst"}
	addSStableAndTestManifest(t, manifest, sstables)
	addSequenceNumberAndTestManifest(t, manifest, sequenceNumbers)

	recoveredData, err := manifest.Recover()
	if err != nil {
		t.Fatalf("unexpected error while recovering manifest: %s", err.Error())
	}

	le := len(sstables)
	lg := len(recoveredData.SSTables)
	if lg != le {
		t.Errorf("unexpected recovered sstable number %d, expetected %d", lg, le)
	}

	for i := range sstables {
		if sstables[i] != recoveredData.SSTables[i] {
			t.Errorf("unexpected sstable path %s recovered, expected %s", recoveredData.SSTables[i], sstables[i])
		}
	}

	sne := sequenceNumbers[len(sequenceNumbers)-1]
	if recoveredData.LastSequenceNumber != sne {
		t.Errorf("recovered last sequence number %d but expected %d", recoveredData.LastSequenceNumber, sne)
	}
}

func TestEmptyRecover(t *testing.T) {
	manifest := openTestManifest(t)

	recoveredData, err := manifest.Recover()
	if err != nil {
		t.Fatalf("unexpected error while recovering empty manifest")
	}

	if recoveredData.LastSequenceNumber != 0 {
		t.Errorf("expected last sequence number to be 0, but got %d", recoveredData.LastSequenceNumber)
	}

	if len(recoveredData.SSTables) != 0 {
		t.Errorf("expected 0 sstables, but got %d", len(recoveredData.SSTables))
	}
}

func openTestManifest(t *testing.T) *ManifestImpl {
	dir := t.TempDir()
	manifest := NewManifestImpl()
	err := manifest.Open(dir)
	if err != nil {
		t.Fatal("failed to open manifest")
	}
	return manifest
}

func addSStableAndTestManifest(t *testing.T, manifest Manifest, tb []string) {
	for _, sst := range tb {
		err := manifest.AddSSTablePath(sst)
		if err != nil {
			t.Errorf("error adding SSTable path %s", sst)
		}
	}
}

func addSequenceNumberAndTestManifest(t *testing.T, manifest Manifest, tb []uint64) {
	for _, sn := range tb {
		err := manifest.AddLatestSequenceNumber(sn)
		if err != nil {
			t.Errorf("error appending sequence number %d", sn)
		}
	}
}

var data RecoverData

func BenchmarkRecoverManifest(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()
		manifest := NewManifestImpl()
		manifest.Open(dir)
		for i := range 10_000 {
			manifest.AddLatestSequenceNumber(uint64(i))
			manifest.AddSSTablePath(fmt.Sprintf("%d.stt", i))
		}
		b.StartTimer()
		data, _ = manifest.Recover()
	}
}

func BenchmarkAddSequenceManifest(b *testing.B) {
	b.ReportAllocs()
	manifest := NewManifestImpl()
	manifest.Open(b.TempDir())
	b.ResetTimer()

	for b.Loop() {
		manifest.AddLatestSequenceNumber(1)
	}
}
