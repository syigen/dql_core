package db

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type CollectionFunc interface {
	Create() error
}
type Collection struct {
	DataBase  *DB
	Dir       string
	Id        CollectionID
	Name      string
	CreatedAt int64
}
type Record struct {
	DB         string
	Collection string
	Id         RecordID
	CreatedAT  int64
	Data       map[string]interface{}
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
