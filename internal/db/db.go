package db

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DataBaseID string
type CollectionID string
type RecordID string

type Options struct {
	Name     string
	ReCreate bool
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
		Name:     "dql",
		ReCreate: false,
	}

	if options != nil {
		opts = *options
	}

	// Prepare Dir
	dir = filepath.Join(dir, opts.Name)
	dir = filepath.Clean(dir)

	// Re Create DB
	if opts.ReCreate {
		_, err := os.Stat(dir)
		if !(err != nil && errors.Is(err, os.ErrNotExist)) {
			err = os.RemoveAll(dir)
			if err != nil {
				return nil, err
			}
		}
	}

	db := DB{
		Dir:         dir,
		Name:        opts.Name,
		mutexes:     make(map[string]*sync.Mutex),
		collections: make(map[string]*Collection),
		CreatedAt:   time.Now().UnixNano(),
	}

	return &db, os.MkdirAll(dir, 0755)
}
