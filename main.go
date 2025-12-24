package main

import (
	"fmt"
	"kendb/db"
	"os"
)

func main() {
	/*
	err := parsers.ParseParquet("small.parquet", new(parsers.RecordEmbedding), 10)
	if err != nil {
		os.Exit(1)
	}
	fmt.Println("Success")*/

	conn, err := db.InitDB("small")
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println(conn.ListTableNames())
	t, ok := conn.GetTableByName("test_run")
	if ok {
		fmt.Println(t.ListColumnNames())
		c, okfr := t.GetColumnByName("video_00")
		if okfr {
			c.PrintColumnEntries()
		}
	}
}
