package db

import (
	"fmt"
	"log"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/sqlparser"
)

type ResultCode int

const (
	PREPARE ResultCode = 100
	SUCCESS            = 200
	ERROR              = 300
)

type Result struct {
	Code    ResultCode
	Message string
	RawSet  RawSet
}

type Engine interface {
	Create(name string) ([]Result, error)
	Query(sqlQuery string) ([]Result, error)
	getMutex(collection string) *sync.Mutex
}
type QueryCollectionDetails struct {
	Name   string
	AsName string
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

func (D DB) Query(sqlQuery string) (Result, error) {
	result := Result{
		Code:    PREPARE,
		Message: "Initiate",
		RawSet:  nil,
	}
	q, err := sqlparser.Parse(sqlQuery)
	if err != nil {
		return result, err
	}
	switch stmt := q.(type) {
	case *sqlparser.Insert:
		rawSet, err := D.insert(stmt)
		if err != nil {
			return Result{}, err
		}
		result.RawSet = rawSet
	case *sqlparser.Select:
		rawSet, err := D.query(stmt)
		if err != nil {
			return Result{}, err
		}
		result.RawSet = rawSet
	default:
		log.Fatalf("%+v", "type mismatch")
	}
	if result.RawSet != nil {
		result.Code = SUCCESS
	} else {
		result.Code = ERROR
	}

	return result, nil
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
