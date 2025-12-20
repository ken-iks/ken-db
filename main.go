package main

import (
	"fmt"
	"kendb/db"
	"os"
)

func main() {
	conn, err := db.InitDB("test")
	if err != nil {
		os.Exit(1)
	}
	conn.AddTable("test1")
	conn.AddTable("test2")
	conn.AddTable("test3")
	fmt.Println(conn.ListTableNames())
	conn.Close()
}