package db

import (
	"fmt"
	"github.com/marianogappa/sqlparser"
	query "github.com/marianogappa/sqlparser/query"
	"log"
	"sync"
	"time"
)

type ResultCode int

const (
	PREPARE ResultCode = 100
	SUCCESS            = 200
	ERROR              = 300
)

type Result struct {
	Response interface{}
	Code     int
	Message  string
}

type Engine interface {
	Create(name string) ([]Result, error)
	Query(sqlQuery string) ([]Result, error)
	getMutex(collection string) *sync.Mutex
}

func (D DB) Create(collection string) (Result, error) {
	result := Result{}

	if collection == "" {
		return result, fmt.Errorf("Missing collection - no place to save record!")
	}

	mutex := D.getMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	c := Collection{
		DataBase:  &D,
		Name:      collection,
		CreatedAt: time.Now().UnixNano(),
	}
	err := c.Create()
	if err != nil {
		return Result{}, err
	}
	D.collections[c.Name] = &c
	result.Message = "Success"
	return result, nil
}

func (D DB) Query(sqlQuery string) ([]Result, error) {
	results := []Result{}
	q, err := sqlparser.Parse(sqlQuery)
	if err != nil {
		return results, err
	}

	collection := q.TableName
	queryType := q.Type

	log.Printf("Collection Name : %s\n", collection)

	switch queryType {
	case query.Insert:
		{
			insertResults, err := D.insert(q.TableName, q.Fields, q.Inserts)
			if err != nil {
				return results, err
			}
			results = append(results, insertResults...)
		}
		break
	case query.Select:
		{

		}
		break
	}

	log.Println("Operation : ", q.Type)

	return results, nil
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modfied to avoid unsafe operations
func (d *DB) getMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}
