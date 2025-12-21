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
	me, err := conn.AddTable("blah", 5)
	if err != nil {
		os.Exit(1)
	}
	me.AddColumn("foo", 10)
	me.AddColumn("bar", 10)
	fmt.Println(conn.ListTableNames())
	fmt.Println(me.ListColumnNames())
	conn.Close()
}