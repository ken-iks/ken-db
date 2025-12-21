package db

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/edsrzf/mmap-go"
)

// InitDB will initialize a database connection either to a new or existing file
// When the file exists, it will connect to it, if not it creates a new one
// Do not forget to defer conn.Close() immediatley after!
func InitDB(filename string) (*DB, error) {
	path := fmt.Sprintf("resources/%s.ken", filename)
	_, err := os.Stat(path)
	var f *os.File
	var fileErr error
	if os.IsNotExist(err) {
		f, fileErr = os.Create(path)
		// initialize the file with 4MB of space
		initialSize := DataRegionStart + ChunkSize
		f.Truncate(int64(initialSize))
	} else {
		f, fileErr = os.OpenFile(path, os.O_RDWR, 0644)
	}
	if fileErr != nil {
		slog.Error("Error, could not open or create file", "file", filename, "error", err)
		return nil, err
	}
	defer f.Close()

	mapped, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		slog.Error("Failed to mmap file", "file", filename, "error", err)
		return nil, err
	}
	cursorPos := GetMetadataCursorPos(mapped)
	if cursorPos == 0 {
		// this means we started a new file, so we set the write offset to 16,
		// set the num tables to 0, and return an empty DB
		ByteOrder.PutUint64(mapped[0:8], MetadataRegionStart) // cursor at position 16
		ByteOrder.PutUint64(mapped[8:16], DataRegionStart) // data region starts 64MB in
		return &DB{
			tables: []*Table{},
			mapped: mapped,
		}, nil
	}

	// In the case that the file already exists, we must load tables and columns
	return &DB{
		tables: loadTables(mapped),
		mapped: mapped,
	}, nil
}

// helper to load all table structs by traversing the mapped bytes
func loadTables(mapped MMap) []*Table {
	tables := []*Table{}
	cursorPos := GetMetadataCursorPos(mapped)
	offset := int64(MetadataRegionStart)
	for offset < cursorPos {
		currTable := Table{
			meta: ReadTableMetadata(mapped, offset),
			columns: []*Column{},
			mapped: mapped,
		}
		offset+=TableMetadataSize

		for range currTable.meta.numColumns {
			columnMeta := ReadColumnMetadata(mapped, offset)
			currTable.columns = append(currTable.columns, &Column{
				meta: columnMeta,
				mapped: mapped,
			})
			offset+=ColumnMetadataSize
		}
		tables = append(tables, &currTable)
	}
	return tables

}

// Close will terminate a database connection and flush the mapped bytes to disk
// Call this anytime you make a call to InitDB
func (conn *DB) Close() error {
	defer conn.mapped.Unmap()
	err := conn.mapped.Flush()
	if err != nil {
		slog.Error("Unable to flush to DB", "error", err)
		return err
	}
	return nil
}


func (conn *DB) AddTable(tablename string, numColumns int) (*Table, error) {
	cursorPos := GetMetadataCursorPos(conn.mapped)
	meta := TableMetadata{
		name: MakeName(tablename),
		numColumns: int64(numColumns),
		offset: cursorPos,
	}
	meta.WriteTo(conn.mapped)

	newTable := Table{
		meta: meta,
		columns: []*Column{},
		mapped: conn.mapped,
	}
	conn.tables = append(conn.tables, &newTable)
	// update metadata cursor position
	cursorPos += TableMetadataSize
	cursorPos += int64(numColumns * ColumnMetadataSize)
	SetMetadataCursorPos(conn.mapped, cursorPos, RIGHT)
	return &newTable, nil
}

func (conn *DB) ListTableNames() []string {
	names := []string{}
	for _, table := range conn.tables {
		names = append(names, table.meta.name.String())
	}
	return names
}