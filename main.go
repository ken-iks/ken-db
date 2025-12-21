package main

import (
	"kendb/db"
	"os"
	"fmt"
)

func main() {
	conn, err := db.InitDB("test")
	if err != nil {
		os.Exit(1)
	}
	me, err := conn.AddTable("cmon", 5)
	if err != nil {
		os.Exit(1)
	}
	fmt.Println(conn.ListTableNames())
	nice, err := me.AddColumn("foo", 2)
	if err != nil {
		os.Exit(1)
	}
	nice.AddVector(0, []float32{5.,7.})
	nice.AddVector(1, []float32{6.,6.})
	nice.AddVector(2, []float32{7.,5.})
	nice.AddVector(3, []float32{8.,4.})
	nice.PrintColumnEntries()
	conn.Close()
}