package db

import (
	"bytes"
	"encoding/binary"

	"github.com/edsrzf/mmap-go"
)

// TODO: rewrite column types for image embed workflows
const (
	Int64Size = 8 // for timestamps
	Float64Size = 8 // for values
	NameSize  = 64
	ColumnMetadataSize = 88 // Name + 3 int64
	TableMetadataSize= 80  // Name + 2 int64
)

// first 8 bytes in the mmaped region are always reserved
// for the current cursorPosition (meaning the next) writeable
// position, and the next 8 bytes are reserved for the number of
// tables in the DB
type MMap = mmap.MMap

// Fixed size for a name
type Name [64]byte

func ReadName(b []byte) Name {
	var n Name
    copy(n[:], b[:64])
    return n
}

func MakeName(s string) Name {
	var n Name
	copy(n[:], s) // zero pad the string
	return n
}

func (n Name) String() string {
    // trim null bytes
    return string(bytes.TrimRight(n[:], "\x00"))
}

type ColumnMetadata struct {
	name Name
	// an actual vector will be this length + 8 (first 8 bytes of a vector is timestamp)
	vectorLength int64
	numVectors int64
	offset int64
}

func ReadColumnMetadata(b []byte, offset int64) ColumnMetadata {
	return ColumnMetadata{
		name: ReadName(b[offset:]),
		vectorLength: int64(ByteOrder.Uint64(b[offset+NameSize:offset+NameSize+8])),
		numVectors: int64(ByteOrder.Uint64(b[offset+NameSize+8:offset+NameSize+16])),
		offset: offset,
	}
}

func (meta *ColumnMetadata) WriteTo(b []byte) {
	offset := meta.offset
	copy(b[offset:], meta.name[:])
	ByteOrder.PutUint64(b[offset+NameSize:], uint64(meta.vectorLength))
	ByteOrder.PutUint64(b[offset+NameSize+8:], uint64(meta.numVectors))
	ByteOrder.PutUint64(b[offset+NameSize+16:], uint64(offset))
}

type TableMetadata struct {
	name Name
	numColumns int64
	offset int64
}

func ReadTableMetadata(b []byte, offset int64) TableMetadata {
	return TableMetadata{
		name: ReadName(b[offset:]),
		numColumns: int64(ByteOrder.Uint64(b[offset+NameSize:offset+NameSize+8])),
		offset: offset,
	}
}

func (meta *TableMetadata) WriteTo(b []byte) {
	offset := meta.offset
	copy(b[offset:], meta.name[:])
	ByteOrder.PutUint64(b[offset+NameSize:], uint64(meta.numColumns))
	ByteOrder.PutUint64(b[offset+NameSize+8:], uint64(offset))
}

type Column struct {
	meta   ColumnMetadata
	mapped MMap
}

type Table struct {
	meta TableMetadata
	columns []*Column
	mapped  MMap
}

type DB struct {
	numTables int64
	cursorPos int64
	tables []*Table
	mapped MMap
}

// centralized byte order for encodings
var ByteOrder = binary.LittleEndian