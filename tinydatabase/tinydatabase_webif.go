package tinydatabase

import (
	//"bytes"
	"encoding/json"
	//"errors"
	"fmt"
	//"io"
	"io/ioutil"
	"net/http"
	//"math"
	//"os"
	//"path"
	//"strconv"
	//"time"
	"regexp"
)

type WebIF struct {
	Prefix    string
	Databases *DatabaseList
}

func AddHandler(webIf WebIF) {
	http.HandleFunc(webIf.Prefix+"databases/", webIf.DispatchHandlerFactory())
}

func (self *WebIF) DispatchHandlerFactory() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == self.Prefix+"databases/" {
			if req.Method == "GET" {
				self.GetDatabaseList(w)
				return
			} else if req.Method == "POST" {
				self.CreateDatabase(w, req)
				return
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
			fmt.Fprint(w, "\"%s\"", key)
		} else {
			fmt.Fprint(w, ",\"%s\"", key)
		}
	}
	fmt.Fprint(w, "]")
}

var regDbName = regexp.MustCompile(`^[A-Za-z0-9_]*$`)

//curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d '{"name":"testdatabase"}'  http://localhost:8000/databases/
//curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d "{\"name\":\"testdatabase\"}"  http://localhost:8000/databases/
//curl -i -H "Content-type: application/json" -X POST -d "{\"name\":\"testdatabase\"}"  http://localhost:8000/databases/

func (self *WebIF) CreateDatabase(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var f interface{}
	body, _ := ioutil.ReadAll(req.Body)
	json.Unmarshal(body, &f)
	m := f.(map[string]interface{})
	dbName := m["name"].(string)
	if regDbName.MatchString(dbName) == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"invalid database name\"}")
		return
	}
	_, ok := self.Databases.Databases[dbName]
	if ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"database exists\"}")
		return
	}
	_, err := self.Databases.NewDatabase(dbName)
	if err != nil {
		fmt.Printf("ERROR:%v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{\"status\":\"ERROR\",\"detail\":\"internal error\"}")
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{\"status\":\"OK\"}")
}
