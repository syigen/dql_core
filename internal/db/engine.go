package db

import (
	"fmt"
	"github.com/marianogappa/sqlparser"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Result struct {
	Response interface{}
	Code     int
	Message  string
}

type Engine interface {
	Create(name string) (Result, error)
	Query(sqlQuery string) (Result, error)
	getMutex(collection string) *sync.Mutex
}

type DB struct {
	Dir     string
	Name    string
	mutex   sync.Mutex
	mutexes map[string]*sync.Mutex
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
		Dir:  dir,
		Name: opts.Name,
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

	//
	dir := filepath.Join(D.Dir, collection)
	dir = filepath.Clean(dir)
	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return result, err
	}

	return result, nil
}

func (D DB) Query(sqlQuery string) (Result, error) {
	result := Result{}
	query, err := sqlparser.Parse(sqlQuery)
	if err != nil {
		return result, err
	}

	collection := query.TableName

	log.Printf("Collection Name : %s", collection)

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
