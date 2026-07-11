# Key Value Store Database Engine using Protobuf!

[![Go Reference](https://pkg.go.dev/badge/golang.org/x/example.svg)](https://pkg.go.dev/golang.org/x/example)

This is a database engine indexed only by key with rather advanced crash recovery methods and datastructures

## Design

Its design is devided in four main pieces:

### 1. Memtable

The memtable keeps the latest records stored in the database in memory, and when its size passes a user-defined treshold, it syncs to disk. In order to ensure availability, it keeps two red black trees in memory so it can switch the active one when flushing to disk. 

### 2. Write Ahead Log

The write ahead log mechanism ensures that data stored in the memtable is persisted to disk evenin case of a crash before syncing. The trade-off is worth it, since appending to a log is much cheaper than indexing a value on disk directly. It ensures that data is not lost when crashing. 

### 3. SSTables 

When the memtable is full, a new sstable is created containing the contents of the active red black tree, which is then cleared. The structure in disk of an sstable is easy: the records are stored continously and a map indexing the keys is stored in the end. The size of that map is stored just after it, therefore in the end of the file, making it possible to find the map. 

### 4. Manifest

The manifest is a simple log structure similar to the write ahead log feature. It just records the currently used sstables and latest sequence number.

## Protobuf! 

Protobuf is the choice for encoding the data due to its performance and simplicity. It was also choose so I could learn more about it. 