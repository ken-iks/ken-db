package db

import (
	"bytes"
	"encoding/binary"

	"github.com/edsrzf/mmap-go"
)

/*
Note:
	Gemini multimodal embeddings api has vector dim: 1408
*/

const (
	Int64Size = 8 // for timestamps
	Float32Size = 4 // for values
	NameSize  = 64
	ColumnMetadataSize = 96 // Name + 4 int64
	TableMetadataSize = 80  // Name + 2 int64
	ChunkSize = 64 * 1024 * 1024
	ChunkHeaderSize = 16
)

const (
    HeaderSize         = 16
    MetadataRegionStart = 16
    DataRegionStart     = 16 * 1024 * 1024  // First 16MB reserved for metadata
)

type Direction int
const (
	LEFT Direction = iota
	RIGHT
)

// first 8 bytes in the mmaped region are always reserved
// for the current cursorPosition (meaning the next) writeable
// position, and the next 8 bytes are reserved for the number of
// tables in the DB
type MMap = mmap.MMap

// Chunk header starts off each chunk
type ChunkHeader struct {
    nextChunk  int64  // offset of next chunk, 0 = last
    numVectors int64  // how many vectors in THIS chunk
}

func ReadChunkHeader(b []byte, offset int64) ChunkHeader {
	return ChunkHeader{
		nextChunk: int64(ByteOrder.Uint64(b[offset:offset+8])),
		numVectors: int64(ByteOrder.Uint64(b[offset+8:offset+16])),
	}
}

func (header *ChunkHeader) WriteTo(b []byte, offset int64) {
	ByteOrder.PutUint64(b[offset:], uint64(header.nextChunk))
	ByteOrder.PutUint64(b[offset+8:], uint64(header.numVectors))
}

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
	firstChunkOffset int64
	offset int64
}

func ReadColumnMetadata(b []byte, offset int64) ColumnMetadata {
	return ColumnMetadata{
		name: ReadName(b[offset:]),
		vectorLength: int64(ByteOrder.Uint64(b[offset+NameSize:offset+NameSize+8])),
		numVectors: int64(ByteOrder.Uint64(b[offset+NameSize+8:offset+NameSize+16])),
		firstChunkOffset: int64(ByteOrder.Uint64(b[offset+NameSize+16:offset+NameSize+24])),
		offset: int64(ByteOrder.Uint64(b[offset+NameSize+24:offset+NameSize+32])),
	}
}

func (meta *ColumnMetadata) WriteTo(b []byte) {
	offset := meta.offset
	copy(b[offset:], meta.name[:])
	ByteOrder.PutUint64(b[offset+NameSize:], uint64(meta.vectorLength))
	ByteOrder.PutUint64(b[offset+NameSize+8:], uint64(meta.numVectors))
	ByteOrder.PutUint64(b[offset+NameSize+16:], uint64(meta.firstChunkOffset))
	ByteOrder.PutUint64(b[offset+NameSize+24:], uint64(meta.offset))
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
	tables []*Table
	mapped MMap
}

func GetMetadataCursorPos(b []byte) int64 {
	return int64(ByteOrder.Uint64(b[:8]))
}

func GetDataCursorPos(b []byte) int64 {
	return int64(ByteOrder.Uint64(b[8:16]))
}

// will set the cursor position if the direction correctly
// matches the relative position of v
// always call when a table or column is added or removed from the DB
func SetMetadataCursorPos(b []byte, v int64, dir Direction) {
	currCursorPos := GetMetadataCursorPos(b)
	if currCursorPos < v && dir == RIGHT {
		ByteOrder.PutUint64(b[:8], uint64(v))
		return
	}
	if currCursorPos > v && dir == LEFT {
		ByteOrder.PutUint64(b[:8], uint64(v))
	}

}

// set every time a new chunk is added or removed from the table
// The next chunk would be at curr + ChunkSize
func SetDataCursorPos(b []byte, v int64, dir Direction) {
	currCursorPos := GetDataCursorPos(b)
	if currCursorPos < v && dir == RIGHT {
		ByteOrder.PutUint64(b[8:16], uint64(v))
		return
	}
	if currCursorPos > v && dir == LEFT {
		ByteOrder.PutUint64(b[8:16], uint64(v))
	}
}

// centralized byte order for encodings
var ByteOrder = binary.LittleEndian