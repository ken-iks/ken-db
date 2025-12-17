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
	} else {
		f, fileErr = os.Open(path)
	}
	if fileErr != nil {
		slog.Error("Error, could not open or create file", "file", filename, "error", err)
		return nil, err
	}
	defer f.Close()

	mapped, _ := mmap.Map(f, mmap.RDWR, 0)

	return &DB{
		tables: []*Table{},
		mapped: mapped,
	}, nil
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
