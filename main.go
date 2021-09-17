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

	_, err = DB.Create("user")

	if err != nil {
		log.Fatal(err)
	}

	results, err := DB.Query("INSERT INTO user (name,age) VALUES ('Namal','2') , ('Amal','5') ")
	if err != nil {
		return
	}

	log.Print(results)

}
