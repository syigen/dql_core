package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type QueryCondition int64

const (
	EQUAL                 QueryCondition = 1
	GREATER_THAN                         = 2
	LESS_THAN                            = 3
	GREATER_THAN_OR_EQUAL                = 4
	LESS_THAN_OR_EQUAL                   = 5
	INVALID                              = 6
)

type Raw map[string]interface{}
type RawSet []Raw
type ResultSet []Result

type CollectionFunc interface {
	Create() error
	Query(columnName string, condition QueryCondition, value interface{}) (RawSet, error)
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
	Raw        Raw
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

func (c *Collection) Query(columnName string, condition QueryCondition, value string) (RawSet, error) {
	rawSet, err := c.search(columnName, condition, value)
	if err != nil {
		return nil, err
	}
	return rawSet, nil
}

func (c *Collection) search(columnName string, condition QueryCondition, value string) (RawSet, error) {
	rawSet := RawSet{}
	err := filepath.WalkDir(c.Dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		file, _ := ioutil.ReadFile(path)
		data := Record{}
		_ = json.Unmarshal([]byte(file), &data)

		rawValue := data.Raw[columnName]
		switch condition {
		case EQUAL:
			if rawValue == value {
				rawSet = append(rawSet, data.Raw)
			}
		case GREATER_THAN:
			if great(rawValue, value) {
				rawSet = append(rawSet, data.Raw)
			}
		case GREATER_THAN_OR_EQUAL:
			if greaterThanOrEq(rawValue, value) {
				rawSet = append(rawSet, data.Raw)
			}
		case LESS_THAN:
			if less(rawValue, value) {
				rawSet = append(rawSet, data.Raw)
			}
		case LESS_THAN_OR_EQUAL:
			if lessThanOrEq(rawValue, value) {
				rawSet = append(rawSet, data.Raw)
			}
		}

		return err
	})
	if err != nil {
		return nil, err
	}
	return rawSet, nil
}
func less(v1 interface{}, v2 string) bool {
	switch v := v1.(type) {
	case int64:
		val, err := strconv.ParseInt(v2, 10, 64)
		if err != nil {
			return false
		}
		return v < val
	case float64:
		val, err := strconv.ParseFloat(v2, 64)
		if err != nil {
			return false
		}
		return v < val
	}
	return false
}
func lessThanOrEq(v1 interface{}, v2 string) bool {
	switch v := v1.(type) {
	case int64:
		val, err := strconv.ParseInt(v2, 10, 64)
		if err != nil {
			return false
		}
		return v <= val
	case float64:
		val, err := strconv.ParseFloat(v2, 64)
		if err != nil {
			return false
		}
		return v <= val
	}
	return false
}
func great(v1 interface{}, v2 string) bool {
	switch v := v1.(type) {
	case int64:
		val, err := strconv.ParseInt(v2, 10, 64)
		if err != nil {
			return false
		}
		return v > val
	case float64:
		val, err := strconv.ParseFloat(v2, 64)
		if err != nil {
			return false
		}
		return v > val
	}
	return false
}
func greaterThanOrEq(v1 interface{}, v2 string) bool {
	switch v := v1.(type) {
	case int64:
		val, err := strconv.ParseInt(v2, 10, 64)
		if err != nil {
			return false
		}
		return v >= val
	case float64:
		val, err := strconv.ParseFloat(v2, 64)
		if err != nil {
			return false
		}
		return v >= val
	}
	return false
}

func ConvertStringToQueryCondition(condition string) (QueryCondition, error) {
	switch condition {
	case "=":
		return EQUAL, nil
	case "<":
		return LESS_THAN, nil
	case "<=":
		return LESS_THAN_OR_EQUAL, nil
	case ">":
		return GREATER_THAN, nil
	case ">=":
		return GREATER_THAN_OR_EQUAL, nil
	}
	return INVALID, fmt.Errorf("invalid Condition %s", condition)
}
