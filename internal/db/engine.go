package db

import (
	"fmt"
	"log"
	"strconv"
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

	switch stmt := q.(type) {
	case *sqlparser.Insert:
		tableName := stmt.Table.Name.String()
		var columns []string

		for _, column := range stmt.Columns {
			columns = append(columns, column.String())
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
						val, _ = strconv.ParseInt(v.Val, 10, 10)
					case sqlparser.FloatVal:
						val, _ = strconv.ParseFloat(v.Val, 10)
					case sqlparser.StrVal:
						val = v.Val
					}
					rowValue = append(rowValue, val)
				}
			}
			rowData = append(rowData, rowValue)
		}

		D.insert(tableName, columns, rowData)
	//checkEqual(t, "users", stmt.TableName)
	case *sqlparser.Select:
		fromTables := stmt.From
		for _, ft := range fromTables {
			switch t := ft.(type) {
			case *sqlparser.AliasedTableExpr:
				tableName := t.Expr.(sqlparser.TableName).Name
				log.Println(tableName)
			}
		}
	default:
		log.Fatalf("%+v", "type mismatch")
	}
	//tableName := q.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	//operator := q.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Operator
	//
	//log.Println(action,tableName,operator)

	//collection := q.TableName
	//queryType := q.Type
	//
	//log.Printf("Collection Name : %s\n", collection)
	//
	//switch queryType {
	//case query.Insert:
	//	{
	//		insertResults, err := D.insert(q.TableName, q.Fields, q.Inserts)
	//		if err != nil {
	//			return results, err
	//		}
	//		results = append(results, insertResults...)
	//	}
	//	break
	//case query.Select:
	//	{
	//
	//		selectQuery, err := D.getResult(q.Fields, q.Conditions)
	//		log.Println(selectQuery)
	//		log.Println(err)
	//
	//	}
	//	break
	//}
	//
	//log.Println("Operation : ", q.Type)

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
