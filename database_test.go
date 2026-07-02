package protobuf_sstable

import "testing"

func TestCreateDatabase(t *testing.T) {
	db := createTestDb(t)
	db.dir = "" // avoit compiler optimization
}

func TestAddDatabase(t *testing.T) {
	db := createTestDb(t)

	err := db.Append("key", "value")
	if err != nil {
		t.Fatalf("unexpected error while inserting to database: %s", err)
	}
}

func TestSearchDatabase(t *testing.T) {
	db := createTestDb(t)
	err := db.Append("key", "value")
	if err != nil {
		t.Fatalf("unexpected error while inserting to database: %s", err)
	}

	found, ok := db.Get("key")
	if !ok {
		t.Errorf("expected to find stored value, but found nothing")
	}

	if found != "value" {
		t.Errorf("found value differs from stored one")
	}
}

func TestDatabaseRecover(t *testing.T) {
	db := createTestDb(t)
	tb := []struct {
		key   string
		value string
		found bool
	}{
		{"key1", "value1", false},
		{"key1", "value12", true},
		{"key2", "value2", false},
		{"", "value1", true},
		{"key2", "value22", true},
	}
	for _, r := range tb {
		if err := db.Append(r.key, r.value); err != nil {
			t.Fatalf("unexpected error while inserting to database: %s", err)
		}
	}

	db2, err := NewDatabase(SetDirectory(db.dir))
	if err != nil {
		t.Fatal("error creating second database")
	}
	for _, r := range tb {
		found, ok := db2.Get(r.key)
		if !ok {
			t.Errorf("failed to retrieved recovered key %s", r.key)
		}
		if r.found && found != r.value {
			t.Errorf("found value does not match with stored key")
		}
	}
}

func createTestDb(t *testing.T) *Database {
	db, err := NewDatabase(SetDirectory(t.TempDir()))
	if err != nil {
		t.Fatalf("unexpected error while creating database: %s", err)
	}
	return db
}
