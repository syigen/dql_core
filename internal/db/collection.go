package db

import (
	"encoding/json"
	"errors"
	"fmt"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
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
