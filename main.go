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

	results, err := DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Namal',2,158,78.5) , ('Amal',5,168,88.5) ")
	if err != nil {
		return
	}

	log.Print(results)

	results, err = DB.Query("SELECT * FROM user WHERE name = 'Test' AND age='1' ")

	log.Println(results)

}
