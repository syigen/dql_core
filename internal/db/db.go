package db

import (
	"encoding/json"
	"fmt"
	"github.com/marianogappa/sqlparser/query"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DataBaseID string
type CollectionID string
type RecordID string

type Options struct {
	Name string
}
type DB struct {
	Id          DataBaseID
	Dir         string
	Name        string
	mutex       sync.Mutex
	mutexes     map[string]*sync.Mutex
	collections map[string]*Collection
	CreatedAt   int64
}

type DbOps interface {
	insert(name string, columns []string, data [][]string) ([]Result, error)
	getResult(columns []string, conditions []query.Condition) ([]Result, error)
}

func New(dir string, options *Options) (*DB, error) {
	//Default Options
	opts := Options{
		Name: "dql",
	}

	if options != nil {
		opts = *options
	}

	// Prepare Dir
	dir = filepath.Join(dir, opts.Name)
	dir = filepath.Clean(dir)

	db := DB{
		Dir:         dir,
		Name:        opts.Name,
		mutexes:     make(map[string]*sync.Mutex),
		collections: make(map[string]*Collection),
		CreatedAt:   time.Now().UnixNano(),
	}

	return &db, os.MkdirAll(dir, 0755)
}

// Insert Record into Collection
func (D DB) insert(name string, columns []string, data [][]interface{}) ([]Result, error) {
	var results []Result
	name, err := formatName(name)
	if err != nil {
		return results, err
	}
	collection := D.collections[name]

	// Create Data Files
	for _, dataRow := range data {
		id, err := gonanoid.New()
		if err != nil {
			return results, err
		}

		// Prepare Record
		record := Record{
			Id:        RecordID(id),
			CreatedAT: time.Now().UnixNano(),
			Data:      make(map[string]interface{}),
		}

		record.Data["id"] = record.Id
		record.Data["collection"] = collection
		record.Data["created_at"] = record.CreatedAT

		for colIndex, value := range dataRow {
			colName := columns[colIndex]
			record.Data[colName] = value
		}

		// Create Data File
		dataFile, _ := json.MarshalIndent(record, "", " ")
		fileName := filepath.Join(collection.DataBase.Dir, collection.Name, string(record.Id))
		err = ioutil.WriteFile(fileName, dataFile, 0644)
		if err != nil {
			return results, err
		}
		results = append(results, Result{
			Response: record,
			Code:     SUCCESS,
			Message:  fmt.Sprintf("Insert Record into %s", record.Collection),
		})
	}

	return results, nil
}

func (D *DB) getResult(columns []string, conditions []query.Condition) ([]Result, error) {

	for _, condition := range conditions {
		log.Println("Operand1", condition.Operand1)
		log.Println("Operand2", condition.Operand2)
		log.Println("Operator", condition.Operator)
		log.Println("Operand1IsField", condition.Operand1IsField)
		log.Println("Operand2IsField", condition.Operand2IsField)
	}

	return nil, nil
}
