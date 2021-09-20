package db

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
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
	Response interface{}
	Code     int
	Message  string
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

func (D DB) Query(sqlQuery string) ([]Result, error) {
	log.Println("-------------------\n", sqlQuery)
	results := []Result{}
	q, err := sqlparser.Parse(sqlQuery)
	if err != nil {
		return results, err
	}
	switch stmt := q.(type) {
	case *sqlparser.Insert:
		tableName := stmt.Table.Name.String()
		var columns []string

		for _, column := range stmt.Columns {
			columns = append(columns, column.Lowered())
		}

		rows := stmt.Rows.(sqlparser.Values)
		var rowData [][]interface{}
		for _, row := range rows {
			var rowValue []interface{}
			for _, datam := range row {
				e := datam.(sqlparser.Expr)
				switch v := e.(type) {
				case *sqlparser.Literal:
					var val interface{}
					switch v.Type {
					case sqlparser.IntVal:
						val, _ = strconv.ParseInt(v.Val, 10, 32)
					case sqlparser.FloatVal:
						val, _ = strconv.ParseFloat(v.Val, 32)
					case sqlparser.StrVal:
						val = v.Val
					}
					rowValue = append(rowValue, val)
				}
			}
			rowData = append(rowData, rowValue)
		}
		results, err = D.insert(tableName, columns, rowData)
		if err != nil {
			return nil, err
		}
	case *sqlparser.Select:
		tables := make(map[string]QueryCollectionDetails)
		for _, from := range stmt.From {
			tb := from.(*sqlparser.AliasedTableExpr)
			colletionName := tb.Expr.(sqlparser.TableName).Name.CompliantName()
			asName := strings.ToLower(colletionName)
			if !tb.As.IsEmpty() {
				asName = tb.As.CompliantName()
			}
			tables[asName] = QueryCollectionDetails{
				Name:   colletionName,
				AsName: asName,
			}
		}

		// Gather column names
		var selectColumns []string
		for _, sExpr := range stmt.SelectExprs {
			switch t := sExpr.(type) {
			case *sqlparser.AliasedExpr:
				colName := strings.ToLower(t.Expr.(*sqlparser.ColName).CompliantName())
				selectColumns = append(selectColumns, colName)
			}
		}

		switch op := stmt.Where.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			colNameWithTableName := strings.ToLower(op.Left.(*sqlparser.ColName).CompliantName())
			colName := strings.ToLower(op.Left.(*sqlparser.ColName).Name.Lowered())
			value := op.Right.(*sqlparser.Literal).Val
			operator := op.Operator.ToString()

			// Select Collection
			var collection *Collection
			if len(tables) == 1 {
				qTable := tables[reflect.ValueOf(tables).MapKeys()[0].String()]
				collection = D.collections[qTable.Name]
			} else {
				for _, tableName := range reflect.ValueOf(tables).MapKeys() {
					if strings.HasPrefix(colNameWithTableName, tableName.String()+"_") {
						qTable := tables[tableName.String()]
						collection = D.collections[qTable.Name]
						break
					}
				}
			}

			condition, err := ConvertStringToQueryCondition(operator)
			if err != nil {
				return nil, err
			}

			if collection != nil {
				resultSet, err := collection.Query(colName, condition, value)
				if err != nil {
					return nil, err
				}
				log.Println(resultSet)
			}
			break
		}

	default:
		log.Fatalf("%+v", "type mismatch")
	}

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
