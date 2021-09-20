package main

import (
	"dcore/internal/db"
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
	for range [10000]int{} {
		result, err = DB.Query("INSERT INTO user (name,age,height,weight) VALUES ('Aruna',15,158,78.5)")
		if err != nil {
			return
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	log.Println("Insert Duration ", diff)
	log.Printf("Insert Duration Per Second %f \n", 10000/diff.Seconds())

	//log.Print(results)
	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Name = 'Namal' ")
	logTime(result, err, t1)

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE > 2 ")
	logTime(result, err, t1)

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.AGE >= 5 ")
	logTime(result, err, t1)

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height < 120  ")
	logTime(result, err, t1)

	t1 = time.Now()
	result, err = DB.Query("SELECT u.name,u.age FROM user as u WHERE u.Height <= 158 ")
	logTime(result, err, t1)

}

func logTime(result db.Result, err error, t1 time.Time) {
	if err != nil {
		log.Fatal(err)
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	log.Printf("Duration %s", diff.String())
	log.Printf("Result Length %d", len(result.RawSet))
	log.Printf("Duration Per Second %f \n", float64(len(result.RawSet))/diff.Seconds())
}
