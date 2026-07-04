package main

import (
	"fmt"

	ps "github.com/bruhugo/protobuf_sstable"
)

func main() {
	db, err := ps.NewDatabase(
		ps.SetMemtableTreshold(10),
		ps.SetDirectory("db/mydb"),
	)
	if err != nil {
		panic(err)
	}

	db.Append("1", "Bruno")
	db.Append("2", "Joe")
	db.Append("3", "Doe")

	name, _ := db.Get("1")

	fmt.Println(name)
}
