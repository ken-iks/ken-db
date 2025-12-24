package main

import (
	"fmt"
	"kendb/db"
	"os"
)

func main() {
	conn, err := db.InitDB("v_10k")
	if err != nil {
		os.Exit(1)
	}
	fmt.Println(conn.ListTableNames())
}
