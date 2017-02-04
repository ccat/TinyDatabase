package tinydatabase

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func Test1_WebifFuncs_basicUsage(t *testing.T) {
	directoryJson := "./testdata_json/"
	//tablename := "testdynamic"
	DirParmission = 0777
	os.RemoveAll(directoryJson)
	//os.Mkdir(directoryJson, 0777)

	webIf := WebIF{}
	webIf.Prefix = "/v1/"
	dbList, err := NewDatabaseList(directoryJson, "json")
	if err != nil {
		t.Errorf("Failed to create new database list:%s", err)
	}
	webIf.Databases = dbList
	/*ts := httptest.NewServer(http.HandlerFunc(webIf.DispatchHandlerFactory()))
	defer ts.Close()*/

	//GET /v1/databases/
	r := httptest.NewRecorder()
	webIf.GetDatabaseList(r)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "[]" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	r = httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v1/databases/", nil)
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	webIf.DispatchHandlerFactory()(r, req)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "[]" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	//POST /v1/databases/
	r = httptest.NewRecorder()
	jsonStr := "{\"name\":\"testdatabase\"}"
	req, err = http.NewRequest("POST", "/v1/databases/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.CreateDatabase(r, req)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"status\":\"OK\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	r = httptest.NewRecorder()
	webIf.GetDatabaseList(r)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "[\"testdatabase\"]" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	r = httptest.NewRecorder()
	jsonStr = "{\"aa\":\"bb\"}"
	req, err = http.NewRequest("POST", "/v1/databases/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.CreateDatabase(r, req)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == http.StatusBadRequest {
		if "{\"status\":\"ERROR\",\"detail\":\"invalid parameter\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	r = httptest.NewRecorder()
	jsonStr = "{\"name\":\"test-database\"}"
	req, err = http.NewRequest("POST", "/v1/databases/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.CreateDatabase(r, req)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == http.StatusBadRequest {
		if "{\"status\":\"ERROR\",\"detail\":\"invalid database name\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	r = httptest.NewRecorder()
	jsonStr = "{\"name\":\"testdatabase2\"}"
	req, err = http.NewRequest("POST", "/v1/databases/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.DispatchHandlerFactory()(r, req)

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"status\":\"OK\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	//POST /v1/databases/<dbname>/tables/
	r = httptest.NewRecorder()
	jsonStr = "{\"name\":\"testtable\",\"type\":\"static\",\"columns\":[{\"name\":\"column1\",\"type\":\"int64\"},{\"name\":\"column2\",\"type\":\"float64\"},{\"name\":\"column3\",\"type\":\"time\"},{\"name\":\"column4\",\"type\":\"string\",\"size\":256}]}"
	req, err = http.NewRequest("POST", "/v1/databases/testdatabase/tables/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.CreateTable(r, req, "testdatabase")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"status\":\"OK\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d,%v", r.Code, string(data))
	}

	r = httptest.NewRecorder()
	jsonStr = "{\"aa\":\"bb\"}"
	req, err = http.NewRequest("POST", "/v1/databases/testdatabase/tables/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	webIf.CreateTable(r, req, "testdatabase")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == http.StatusBadRequest {
		if "{\"status\":\"ERROR\",\"detail\":\"Invalid parameter of table name\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d,%v", r.Code, string(data))
	}

	//GET /v1/databases/<dbname>/tables/
	r = httptest.NewRecorder()
	req, err = http.NewRequest("GET", "/v1/databases/testdatabase/tables/", nil)
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	webIf.GetTableList(r, "testdatabase")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "[\"testtable\"]" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	//GET /v1/databases/<dbname>/tables/<tablename>/
	r = httptest.NewRecorder()
	req, err = http.NewRequest("GET", "/v1/databases/testdatabase/tables/testtable/", nil)
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	webIf.GetTableDetail(r, "testdatabase", "testtable")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"name\":\"testtable\",\"database\":\"testdatabase\",\"type\":\"static\",\"columns\":[{\"name\":\"column1\",\"type\":\"int64\",\"size\":64},{\"name\":\"column2\",\"type\":\"float64\",\"size\":64},{\"name\":\"column3\",\"type\":\"time\",\"size\":15},{\"name\":\"column4\",\"type\":\"string\",\"size\":256}]}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d", r.Code)
	}

	//POST /v1/databases/<dbname>/tables/<tablename>/rows/
	r = httptest.NewRecorder()
	jsonStr = "{\"column1\":1,\"column2\":1.2,\"column3\":\"2013-06-19 21:54:23 +0900\",\"column4\":\"strings\"}"
	req, err = http.NewRequest("POST", "/v1/databases/testdatabase/tables/testtable/rows/", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		t.Errorf("Failed to create req:%s", err)
	}
	webIf.AddRow(r, req, "testdatabase", "testtable")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"status\":\"OK\",\"rownum\":0}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d,%v", r.Code, string(data))
	}

	//GET /v1/databases/<dbname>/tables/<tablename>/rows/<num>/
	r = httptest.NewRecorder()
	webIf.GetRow(r, "testdatabase", "testtable", "0")

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Error by ioutil.ReadAll(). %v", err)
	}

	if r.Code == 200 {
		if "{\"column1\":1,\"column2\":1.200000,\"column3\":\"2013-06-19T21:54:23+09:00\",\"column4\":\"strings\"}" != string(data) {
			t.Fatalf("Data Error. %v", string(data))
		}
	} else {
		t.Fatalf("Status Error %d,%v", r.Code, string(data))
	}
}
