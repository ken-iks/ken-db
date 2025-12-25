package db

import (
	"fmt"
	"log/slog"
)

// InitDB will initialize a database connection either to a new or existing file
// When the file exists, it will connect to it, if not it creates a new one
// Do not forget to defer conn.Close() immediatley after!
func InitDB(filename string) (*DB, error) {
	path := fmt.Sprintf("resources/%s.ken", filename)
	initialSize := int64(DataRegionStart + (ChunkSize * 4)) //~270MB

	file, err := OpenMMapFile(path, initialSize)
	if err != nil {
		slog.Error("Failed to open mmap file", "file", filename, "error", err)
		return nil, err
	}

	b := file.Bytes()
	cursorPos := GetMetadataCursorPos(b)
	if cursorPos == 0 {
		// this means we started a new file, so we set the write offset to 16,
		// set the num tables to 0, and return an empty DB
		ByteOrder.PutUint64(b[0:8], MetadataRegionStart) // cursor at position 16
		ByteOrder.PutUint64(b[8:16], DataRegionStart)    // data region starts 16MB in
		return &DB{
			tables: []*Table{},
			file:   file,
		}, nil
	}

	// In the case that the file already exists, we must load tables and columns
	return &DB{
		tables: loadTables(file),
		file:   file,
	}, nil
}

// helper to load all table structs by traversing the mapped bytes
func loadTables(file *MMapFile) []*Table {
	tables := []*Table{}
	b := file.Bytes()
	cursorPos := GetMetadataCursorPos(b)
	offset := int64(MetadataRegionStart)
	for offset < cursorPos {
		currTable := Table{
			meta:    ReadTableMetadata(b, offset),
			columns: []*Column{},
			file:    file,
		}
		offset += TableMetadataSize

		for range currTable.meta.numColumns {
			columnMeta := ReadColumnMetadata(b, offset)
			currTable.columns = append(currTable.columns, &Column{
				meta: columnMeta,
				file: file,
			})
			offset += ColumnMetadataSize
		}
		tables = append(tables, &currTable)
	}
	return tables
}

// Close will terminate a database connection and flush the mapped bytes to disk
// Call this anytime you make a call to InitDB
func (conn *DB) Close() error {
	err := conn.file.Close()
	if err != nil {
		slog.Error("Unable to flush to DB", "error", err)
		return err
	}
	return nil
}

func (conn *DB) AddTable(tablename string, numColumns int) (*Table, error) {
	b := conn.file.Bytes()
	cursorPos := GetMetadataCursorPos(b)
	meta := TableMetadata{
		name:       MakeName(tablename),
		numColumns: int64(numColumns),
		offset:     cursorPos,
	}
	meta.WriteTo(b)

	newTable := Table{
		meta:    meta,
		columns: []*Column{},
		file:    conn.file,
	}
	conn.tables = append(conn.tables, &newTable)
	// update metadata cursor position
	cursorPos += TableMetadataSize
	cursorPos += int64(numColumns * ColumnMetadataSize)
	SetMetadataCursorPos(b, cursorPos, RIGHT)
	return &newTable, nil
}

func (conn *DB) GetTableByName(name string) (*Table, bool) {
	for _, table := range conn.tables {
		if table.meta.name.String() == name {
			return table, true
		}
	}
	return nil, false
}

func (conn *DB) ListTableNames() []string {
	names := []string{}
	for _, table := range conn.tables {
		names = append(names, table.meta.name.String())
	}
	return names
}
