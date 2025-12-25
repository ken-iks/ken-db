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
// * NOTE: This must take a *pointer* to a struct that implements the record interface
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
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		pf, err := parquet.OpenFile(f, fi.Size())
		if err != nil {
			return err
		}

		rowCount := 0
		for _, rowGroup := range pf.RowGroups() {
			rows := rowGroup.Rows()
			defer rows.Close()

			// Buffer for reading rows (read 100 at a time)
			rowBuf := make([]parquet.Row, 100)

			for {
				n, err := rows.ReadRows(rowBuf)
				if err != nil && err != io.EOF {
					slog.Error(err.Error())
					return err
				}

				for i := 0; i < n; i++ {
					row := rowBuf[i]
					// Column order: Video_id (0), Timestamp (1), Embedding (2)
					videoID := row[0].String()
					timestamp := row[1].Int64()
					embedding := row[2].ByteArray()

					slog.Info(
						"Adding record", "VideoId=", videoID, "Timestamp=", timestamp, "Vector length=", len(embedding)/4,
					)

					// Skip any records with empty embeddings
					if len(embedding) == 0 {
						slog.Warn("Skipping record with empty embedding", "VideoID", videoID)
						rowCount++
						continue
					}

					col, ok := columnsByName[videoID]
					if !ok {
						col, _ = tbl.AddColumn(videoID, int64(len(embedding)/4))
						columnsByName[videoID] = col
					}
					writer := db.WriteColumnOptions{Kind: db.Bytes}
					writer.AddBytes(embedding)
					col.AddVector(timestamp, writer)
					rowCount++
				}

				if err == io.EOF {
					break
				}
			}
		}
		return nil
	case RecordURI:
		return fmt.Errorf("not implemented yet")
	default:
		slog.Error("Unknows record type in parquet file", "type:", v)
		return fmt.Errorf("Bruh")
	}
}
