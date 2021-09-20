package db

import (
	"encoding/json"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/sqlparser"
)

type DbOps interface {
	_insert(name string, columns []string, data [][]string) (RawSet, error)
	insert(stmt *sqlparser.Insert) (RawSet, error)
	query(stmt *sqlparser.Select) (RawSet, error)
}

// Insert Record into Collection
func (D *DB) _insert(name string, columns []string, data [][]interface{}) (RawSet, error) {
	var results RawSet
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
			Id:         RecordID(id),
			CreatedAT:  time.Now().UnixNano(),
			Collection: collection.Name,
			DB:         D.Name,
			Raw:        make(map[string]interface{}),
		}

		record.Raw["id"] = record.Id
		record.Raw["created_at"] = record.CreatedAT

		for colIndex, value := range dataRow {
			colName := columns[colIndex]
			record.Raw[colName] = value
		}

		// Create Data File
		dataFile, _ := json.MarshalIndent(record, "", " ")
		fileName := filepath.Join(collection.DataBase.Dir, collection.Name, string(record.Id))
		err = ioutil.WriteFile(fileName, dataFile, 0644)
		if err != nil {
			return results, err
		}
		results = append(results, record.Raw)
	}

	return results, nil
}

func (D *DB) insert(stmt *sqlparser.Insert) (RawSet, error) {
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
	rawSet, err := D._insert(tableName, columns, rowData)
	if err != nil {
		return rawSet, err
	}
	return nil, nil
}

func (D *DB) query(stmt *sqlparser.Select) (RawSet, error) {
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
			return resultSet, nil
		}
	}
	return nil, nil
}
