package tinydatabase

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type WebIF struct {
	Prefix    string
	Databases *DatabaseList
}

type tableJson struct {
	Name    string
	Types   string
	Columns []ColumnType
}

var (
	ErrInvalidParam          = errors.New("Invalid parameter")
	ErrInvalidParamTableName = errors.New("Invalid parameter of table name")
	ErrInvalidParamTableType = errors.New("Invalid parameter of table type")
	ErrInvalidParamColumn    = errors.New("Invalid parameter of columns")
)

func AddHandler(webIf WebIF) {
	http.HandleFunc(webIf.Prefix+"databases/", webIf.DispatchHandlerFactory())
}

func (self *WebIF) DispatchHandlerFactory() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == self.Prefix+"databases/" {
			if req.Method == "GET" {
				fmt.Printf("GET %s\n", req.URL.Path)
				self.GetDatabaseList(w)
				return
			} else if req.Method == "POST" {
				fmt.Printf("POST %s\n", req.URL.Path)
				self.CreateDatabase(w, req)
				return
			}
		} else if strings.HasPrefix(req.URL.Path, self.Prefix+"databases/") {
			afterWords := req.URL.Path[len(self.Prefix+"databases/"):]
			commands := strings.Split(afterWords, "/")
			databaseName := commands[0]
			if regDbName.MatchString(databaseName) == false {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid database name\"}")
				return
			}
			if len(commands) == 2 {
				if commands[1] != "tables/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				if req.Method == "GET" {
					fmt.Printf("GET %s\n", req.URL.Path)
					self.GetTableList(w, databaseName)
					return
				} else if req.Method == "POST" {
					fmt.Printf("POST %s\n", req.URL.Path)
					self.CreateTable(w, req, databaseName)
				}
			} else if len(commands) == 3 {
				if commands[1] != "tables/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				tableName := commands[2]
				if regDbName.MatchString(tableName) == false {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid table name\"}")
					return
				}
				if req.Method == "GET" {
					fmt.Printf("GET %s\n", req.URL.Path)
					self.GetTableDetail(w, databaseName, tableName)
					return
				}
			} else if len(commands) == 4 {
				if commands[1] != "tables/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				tableName := commands[2]
				if regDbName.MatchString(tableName) == false {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid table name\"}")
					return
				}
				if commands[3] != "rows/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				if req.Method == "POST" {
					fmt.Printf("POST %s\n", req.URL.Path)
					self.AddRow(w, req, databaseName, tableName)
					return
				}
			} else if len(commands) == 5 {
				if commands[1] != "tables/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				tableName := commands[2]
				if regDbName.MatchString(tableName) == false {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid table name\"}")
					return
				}
				if commands[3] != "rows/" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid path\"}")
					return
				}
				rowNum := commands[4]
				if req.Method == "GET" {
					fmt.Printf("GET %s\n", req.URL.Path)
					self.GetRow(w, databaseName, tableName, rowNum)
					return
				}
			}
		}
	}
}

func (self *WebIF) GetDatabaseList(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "[")
	firstFlag := true
	for key, _ := range self.Databases.Databases {
		if firstFlag {
			fmt.Fprintf(w, "\"%s\"", key)
			firstFlag = false
		} else {
			fmt.Fprintf(w, ",\"%s\"", key)
		}
	}
	fmt.Fprint(w, "]")
}

var regDbName = regexp.MustCompile(`^[A-Za-z0-9_]*$`)

//curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d '{"name":"testdatabase"}'  http://localhost:8000/databases/
//curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d "{\"name\":\"testdatabase\"}"  http://localhost:8000/v1/databases/

func (self *WebIF) CreateDatabase(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var f interface{}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	fmt.Printf("  %s\n", string(body))
	json.Unmarshal(body, &f)
	m, ok := f.(map[string]interface{})
	if ok == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	dbNameI, ok := m["name"]
	if ok == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	dbName, ok := dbNameI.(string)
	if ok == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	if regDbName.MatchString(dbName) == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid database name\"}")
		return
	}
	_, err = self.Databases.Get(dbName)
	if err == nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"database exists\"}")
		return
	}
	_, err = self.Databases.NewDatabase(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"internal error\"}")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{\"status\":\"OK\"}")
}

func (self *WebIF) GetTableList(w http.ResponseWriter, dbName string) {
	w.Header().Set("Content-Type", "application/json")

	db, err := self.Databases.Get(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"no database\"}")
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "[")
	firstFlag := true
	for key, _ := range db.tables {
		if firstFlag {
			firstFlag = false
		} else {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "\"%s\"", key)
	}
	fmt.Fprint(w, "]")
}

func (self *WebIF) CreateTable(w http.ResponseWriter, req *http.Request, dbName string) {
	w.Header().Set("Content-Type", "application/json")

	db, err := self.Databases.Get(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"no database\"}")
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	fmt.Printf("  %s\n", string(body))
	tableJ, err := self.jsonChecker_CreateTable(body)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "{\"status\":\"ERROR\",\"detail\":\"%s\"}", err)
		return
	}
	_, err = db.GetTable(tableJ.Name)
	if err == nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"table exist\"}")
		return
	}
	_, err = db.NewTable(tableJ.Name, tableJ.Types, tableJ.Columns)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"internal error\"}")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{\"status\":\"OK\"}")
}

func (self *WebIF) jsonChecker_CreateTable(body []byte) (*tableJson, error) {
	var f interface{}
	json.Unmarshal(body, &f)
	result := &tableJson{}
	m, ok := f.(map[string]interface{})
	if ok == false {
		return nil, ErrInvalidParam
	}
	tablenameI, ok := m["name"]
	if ok == false {
		return nil, ErrInvalidParamTableName
	}
	result.Name, ok = tablenameI.(string)
	if ok == false {
		return nil, ErrInvalidParamTableName
	}
	if regDbName.MatchString(result.Name) == false {
		return nil, ErrInvalidParamTableName
	}

	tableTypeI, ok := m["type"]
	if ok == false {
		return nil, ErrInvalidParamTableType
	}
	result.Types, ok = tableTypeI.(string)
	if ok == false {
		return nil, ErrInvalidParamTableType
	}
	if result.Types != "static" && result.Types != "dynamic" {
		return nil, ErrInvalidParamTableType
	}

	columnsI, ok := m["columns"]
	if ok == false {
		return nil, ErrInvalidParamColumn
	}
	columnsList, ok := columnsI.([]interface{})
	if ok == false {
		return nil, ErrInvalidParamColumn
	}
	for i, val := range columnsList {
		column := ColumnType{}
		valMap, ok := val.(map[string]interface{})
		if ok == false {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + " is invalid")
		}
		nameI, ok := valMap["name"]
		if ok == false {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + " name is invalid")
		}
		column.Name, ok = nameI.(string)
		if ok == false {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + " name is invalid")
		}
		typeI, ok := valMap["type"]
		if ok == false {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + "(" + column.Name + ") type is invalid")
		}
		column.Type, ok = typeI.(string)
		if ok == false {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + "(" + column.Name + ") type is invalid")
		}
		if column.Type == COLUMN_INT64 {
			column.Size = 64
		} else if column.Type == COLUMN_FLOAT64 {
			column.Size = 64
		} else if column.Type == COLUMN_STRING {
			sizeI, ok := valMap["size"]
			if ok == false {
				return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + "(" + column.Name + ") size is invalid")
			}
			sizeF, ok := sizeI.(float64)
			if ok == false {
				return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + "(" + column.Name + ") size is invalid")
			}
			column.Size = int64(sizeF)
		} else if column.Type == COLUMN_TIME {
			column.Size = 15
		} else {
			return nil, errors.New("column " + strconv.FormatInt(int64(i+1), 10) + "(" + column.Name + ") type is invalid")
		}
		result.Columns = append(result.Columns, column)
	}
	return result, nil
}

func (self *WebIF) GetTableDetail(w http.ResponseWriter, dbName string, tableName string) {
	w.Header().Set("Content-Type", "application/json")

	db, err := self.Databases.Get(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"no database\"}")
		return
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"table does not exist\"}")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{\"name\":\"%s\",\"database\":\"%s\",", tableName, dbName)
	fmt.Fprintf(w, "\"type\":\"%s\",\"columns\":[", table.GetTableType())
	firstFlag := true
	for _, val := range table.GetColumns() {
		if firstFlag {
			firstFlag = false
		} else {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "{\"name\":\"%s\",\"type\":\"%s\",\"size\":%d}", val.Name, val.Type, val.Size)
	}
	fmt.Fprint(w, "]}")
}

func (self *WebIF) AddRow(w http.ResponseWriter, req *http.Request, dbName string, tableName string) {
	w.Header().Set("Content-Type", "application/json")

	db, err := self.Databases.Get(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"no database\"}")
		return
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"table does not exist\"}")
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	var f interface{}
	json.Unmarshal(body, &f)
	m, ok := f.(map[string]interface{})
	if ok == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	tempC := table.GetColumns()
	tableCslice := []string{}
	for _, val := range tempC {
		tableCslice = append(tableCslice, val.Name)
	}
	sort.Strings(tableCslice)
	inputCslice := []string{}
	for key, _ := range m {
		inputCslice = append(inputCslice, key)
	}
	sort.Strings(inputCslice)
	if reflect.DeepEqual(tableCslice, inputCslice) == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter. table:%v, input:%v\"}", tableCslice, inputCslice)
		return
	}
	for _, val := range tempC {
		_, err = val.ConvertToBytes(m[val.Name])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter for %s\"}", val.Name)
			return
		}
	}
	rowNum, err := table.WriteRow(m)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"internal server error\"}")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{\"status\":\"OK\",\"rownum\":%d}", rowNum)
}

func (self *WebIF) GetRow(w http.ResponseWriter, dbName string, tableName string, rowNum string) {
	w.Header().Set("Content-Type", "application/json")

	db, err := self.Databases.Get(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"no database\"}")
		return
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"table does not exist\"}")
		return
	}
	rowNumI, err := strconv.ParseInt(rowNum, 10, 64)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}")
		return
	}
	row, err := table.ReadRow(rowNumI)
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"internal server error\"}")
		return
	}
	columns := table.GetColumns()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{")
	for i, val := range columns {
		if i != 0 {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "\"%s\":", val.Name)
		if val.Type == COLUMN_STRING {
			fmt.Fprintf(w, "\"%s\"", row[val.Name].(string))
		} else if val.Type == COLUMN_INT64 {
			fmt.Fprintf(w, "%d", row[val.Name].(int64))
		} else if val.Type == COLUMN_FLOAT64 {
			fmt.Fprintf(w, "%f", row[val.Name].(float64))
		} else if val.Type == COLUMN_TIME {
			fmt.Fprintf(w, "\"%s\"", row[val.Name].(time.Time).Format(time.RFC3339Nano))
		}
	}
	fmt.Fprint(w, "}")

}
