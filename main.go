package main

import (
	"dcore/internal/db"
	"log"
)

func main() {
	DB, err := db.New("_build/db", &db.Options{Name: "test", ReCreate: true})
	if err != nil {
		log.Fatal(err)
	}
	log.Print(DB)

	_, err = DB.Create("user")

	if err != nil {
		log.Fatal(err)
	}

	results, err := DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Aruna',1.5,158,78.5) ,('Namal',2,108,78.5) , ('Amal',5,168,88.5) , ('Yas',15,168,88.5) ")
	if err != nil {
		return
	}

	//log.Print(results)

	results, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Name = 'Namal' ")
	log.Println(results)
	results, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE > 2 ")
	log.Println(results)
	results, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE >= 5 ")
	log.Println(results)
	results, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height < 120  ")
	log.Println(results)
	results, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height <= 158 ")

	log.Println(results)

}
