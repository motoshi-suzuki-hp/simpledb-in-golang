package main

import (
	"fmt"
	"log"
	"simpledb-in-golang/file"
)

func main() {
	const (
		dbName    = "filetest"
		blockSize = 400
	)

	fm, err := file.NewFileMgr(dbName, blockSize)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("isNew:", fm.IsNew())

	blk := file.NewBlockId("testfile", 2)

	// write
	p1 := file.NewPage(fm.BlockSize())
	pos1 := 88
	if err := p1.SetString(pos1, "abcdefghijklm"); err != nil {
		log.Fatal(err)
	}
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	if err := p1.SetInt(pos2, 345); err != nil {
		log.Fatal(err)
	}
	if err := fm.Write(blk, p1); err != nil {
		log.Fatal(err)
	}

	// read
	p2 := file.NewPage(fm.BlockSize())
	if err := fm.Read(blk, p2); err != nil {
		log.Fatal(err)
	}
	ival, _ := p2.GetInt(pos2)
	str, _ := p2.GetString(pos1)
	fmt.Printf("offset %d contains %d\n", pos2, ival)
	fmt.Printf("offset %d contains %q\n", pos1, str)
}
