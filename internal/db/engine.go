package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/marianogappa/sqlparser"
	query "github.com/marianogappa/sqlparser/query"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
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

type CollectionFunc interface {
	formatName(collection string) error
	Create() error
}

type DataBaseID string
type CollectionID string
type RecordID string

type Record struct {
	DB         string
	Collection string
	Id         RecordID
	CreatedAT  int64
	Data       map[string]interface{}
}

type Collection struct {
	DataBase  *DB
	Dir       string
	Id        CollectionID
	Name      string
	CreatedAt int64
}

type Engine interface {
	Create(name string) ([]Result, error)
	Query(sqlQuery string) ([]Result, error)
	getMutex(collection string) *sync.Mutex
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

type Options struct {
	Name string
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

// Insert Record into Collection
func (D DB) insert(name string, columns []string, data [][]string) ([]Result, error) {
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

func formatName(collection string) (string, error) {
	if collection == "" {
		return "", errors.New("collection Name should not empty")
	}
	return strings.ToLower(collection), nil
}
func (c *Collection) Create() error {
	// Format Collection Name
	collection, err := formatName(c.Name)
	if err != nil {
		return err
	}
	c.Name = collection

	// Prepare Collection Name
	dir := filepath.Join(c.DataBase.Dir, c.Name)
	dir = filepath.Clean(dir)

	// Create Collection Dir If not exists
	_, err = os.Stat(dir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// create collection directory
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		log.Printf("Collection %s Created At %d", c.Name, c.CreatedAt)
	}
	c.Dir = dir

	return nil
}
