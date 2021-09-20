package main

import (
	"dcore/internal/db"
	"fmt"
	"log"
	"time"
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
	var result db.Result

	t1 := time.Now()

	for range [1000]int{} {
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Aruna',15,158,78.5)")
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Amila',16,148,68.5)")
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Yasruka',25,168,58.5)")
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Maduranga',45,172,72.5)")
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Aruna',55,160,72.5)")
		if err != nil {
			return
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Println("Insert Duration ", diff)
	fmt.Println("Insert Duration Per Second\n", 10000*5/diff.Seconds())

	//log.Print(results)
	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Name = 'Namal' ")
	log.Println(len(result.RawSet))
	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("Duration ", diff)
	fmt.Println("Duration Per Second\n", 1000000*5/diff.Seconds())

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE > 2 ")
	log.Println(len(result.RawSet))

	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("Duration ", diff)
	fmt.Println("Duration Per Second\n", 1000000*5/diff.Seconds())

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE >= 5 ")
	log.Println(len(result.RawSet))

	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("Duration ", diff)
	fmt.Println("Duration Per Second\n", 1000000*5/diff.Seconds())

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height < 120  ")
	log.Println(len(result.RawSet))
	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("Duration ", diff)
	fmt.Println("Duration Per Second\n", 1000000*5/diff.Seconds())

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height <= 158 ")
	log.Println(len(result.RawSet))
	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("Duration ", diff)
	fmt.Println("Duration Per Second", 1000000*5/diff.Seconds())

}
