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
		f.Truncate(int64(os.Getpagesize() * 1024))
	} else {
		f, fileErr = os.Open(path)
	}
	if fileErr != nil {
		slog.Error("Error, could not open or create file", "file", filename, "error", err)
		return nil, err
	}
	defer f.Close()

	mapped, _ := mmap.Map(f, mmap.RDWR, 0)
	cursorPos := ByteOrder.Uint64(mapped[:8])
	numTables := ByteOrder.Uint64(mapped[8:16])
	if cursorPos == 0 {
		if numTables != 0 {
			panic("Invariant violated: Cursor position set to 0 but non zero num tables")
		}

		// this means we started a new file, so we set the write offset to 16,
		// set the num tables to 0, and return an empty DB
		ByteOrder.PutUint64(mapped[0:8], 16) // cursor at position 16
		ByteOrder.PutUint64(mapped[8:16], 0) // num tables set to 0
		return &DB{
			numTables: 0,
			cursorPos: 0,
			tables: []*Table{},
			mapped: mapped,
		}, nil
	}

	// In the case that the file already exists, we must load tables and columns
	return &DB{
		numTables: int64(numTables),
		cursorPos: int64(cursorPos),
		tables: loadTables(mapped, int64(numTables), int64(cursorPos)),
		mapped: mapped,
	}, nil
}

// helper to load all table structs by traversing the mapped bytes
func loadTables(mapped MMap, numTables int64, cursorPos int64) []*Table {
	tables := []*Table{}
	
	offset := int64(16)
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
			offset+=(columnMeta.numVectors*columnMeta.vectorLength)
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


func (conn *DB) AddTable(tablename string) (*Table, error) {
	meta := TableMetadata{
		name: MakeName(tablename),
		numColumns: 0,
		offset: conn.cursorPos,
	}
	meta.WriteTo(conn.mapped)
	conn.cursorPos += TableMetadataSize
	newTable := Table{
		meta: meta,
		columns: []*Column{},
		mapped: conn.mapped,
	}
	conn.tables = append(conn.tables, &newTable)
	return &newTable, nil
}

func (conn *DB) ListTableNames() []string {
	names := []string{}
	for _, table := range conn.tables {
		names = append(names, table.meta.name.String())
	}
	return names
}