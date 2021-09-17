package db

import (
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
