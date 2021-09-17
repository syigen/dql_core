package main

import (
	"dcore/internal/db"
	"log"
)

func main() {
	DB, err := db.New("_build/db", nil)
	if err != nil {
		return
	}
	log.Print(DB)

	result, err := DB.Query("CREATE TABLE User")

	if err != nil {
		log.Fatal(err)
	}
	log.Print(result)
}
