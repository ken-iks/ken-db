package parsers

import (
	"fmt"
	"io"
	"kendb/db"
	"log/slog"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Parses a .parquet file to a .ken file - based on the record interface
// For filename data.parquet, the eqivalent data.ken file will be created
// You can use db.Init to initialize a DB connection to this file
func ParseParquet(filename string, r Record, colCount int) error {
	sl := strings.Split(filename, ".")
	if sl[len(sl)-1] != "parquet" {
		slog.Error("Invalid format. Please ensure parquet file has appropriate file extension")
		return fmt.Errorf("Bruh")
	}

	path := fmt.Sprintf("external/%s", filename)
	f, err := os.Open(path)
	if err != nil {
		slog.Error(err.Error())
		return err
	}
	defer f.Close()

	conn, err := db.InitDB(sl[0])
	if err != nil {
		slog.Error("Could not initalize db connection", "filename:", filename)
		return err
	}
	defer conn.Close()

	tbl, err := conn.AddTable("test_run", colCount)
	if err != nil {
		return err
	}

	columnsByName := make(map[string]*db.Column)

	switch v := r.(type) {
	case *RecordEmbedding:
		reader := parquet.NewGenericReader[RecordEmbedding](f)
		defer reader.Close()
		records := make([]RecordEmbedding, 1000)

		for {
			n, err := reader.Read(records)
			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Error(err.Error())
				return err
			}

			for i := 0; i < n; i++ {
				r := records[i]
				col, ok := columnsByName[r.VideoID]
				if !ok {
					col, _ = tbl.AddColumn(r.VideoID, int64(len(r.Embedding)))
					columnsByName[r.VideoID] = col
				}
				writer := db.WriteColumnOptions{Kind: db.Floats}
				writer.AddFloats(r.Embedding)
				col.AddVector(r.Timestamp, writer)
			}

			fmt.Printf("Processed %d records\n", n)
		}
		return nil
	case RecordURI:
		return fmt.Errorf("not implemented yet")
	default:
		slog.Error("Unknows record type in parquet file", "type:", v)
		return fmt.Errorf("Bruh")
	}
}
