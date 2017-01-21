package tinydatabase

import (
	//"fmt"
	"os"
	//"path"
	//"strings"
	"testing"
	"time"
)

func Test1_database_new_json(t *testing.T) {
	fakeDirectory := "./testdatafile"
	directoryJson := "./testdata_json/"
	//tablename := "testdynamic"
	DirParmission = 0777
	os.RemoveAll(directoryJson)
	//os.Mkdir(directoryJson, 0777)

	_, err := NewDatabaseList(fakeDirectory, "json")
	if err == nil {
		t.Errorf("1.Failed to check directory")
	}
	if err != ErrNotDir {
		t.Errorf("2.Failed to check directory. Unexpected err:%s", err)
	}
	_, err = NewDatabaseList(directoryJson, "nojson")
	if err == nil {
		t.Errorf("3.Failed to check filetype")
	}
	if err != ErrInvalidFiletype {
		t.Errorf("4.Failed to check filetype. Unexpected err:%s", err)
	}
	dbAllInstJson, err := NewDatabaseList(directoryJson, "json")
	if err != nil {
		t.Errorf("5.Failed to create new database list:%s", err)
	}
	dbInst1, err := dbAllInstJson.NewDatabase("database1")
	if err != nil {
		t.Errorf("6.Failed to create new database:%s", err)
	}
	_, err = dbAllInstJson.NewDatabase("database2")
	if err != nil {
		t.Errorf("7.Failed to create new database:%s", err)
	}
	_, err = dbAllInstJson.NewDatabase("database2")
	if err == nil || err == ErrNotImplemented {
		t.Errorf("8.Failed to refuse to create new database:%s", err)
	}

	dbInst1temp, err := dbAllInstJson.Get("database1")
	if err != nil {
		t.Errorf("9.Failed to get database:%s", err)
	}
	if dbInst1 != dbInst1temp {
		t.Errorf("10.Failed to get database1:%v", dbInst1temp)
	}

	columnSet := []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time", Size: 15},
	}

	_, err = dbInst1.NewTable("table1", "notype", columnSet)
	if err == nil || err != ErrInvalidTabletype {
		t.Errorf("Failed to prevent non type table")
	}

	table1, err := dbInst1.NewTable("table1", "static", columnSet)

	if err != nil {
		t.Errorf("Failed to create table: %s", err)
	}
	_, err = os.Stat(directoryJson + "/database1/" + "table1" + ".table")
	if err != nil {
		t.Errorf("Failed to create table file:%s", err)
	}
	_, err = os.Stat(directoryJson + "/database1/" + "table1" + ".config")
	if err != nil {
		t.Errorf("Failed to create config file:%s", err)
	}

	testRow := make(Row)
	testRow["intline"] = int64(100)
	testRow["floatline"] = 10.5
	testRow["strline"] = "aaaa"
	testRow["dateline"] = time.Now()

	num, err := table1.WriteRow(testRow)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 0 {
		t.Errorf("Failed to insert row at 0: %d", num)
	}
	_, err = dbInst1.NewTable("table1", "static", columnSet)
	if err == nil {
		t.Errorf("Failed to prevent creating same table")
	}

	columnSet = []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 0},
		{Name: "dateline", Type: "time", Size: 15},
	}
	_, err = dbInst1.NewTable("table2", "dynamic", columnSet)
	if err != nil {
		t.Errorf("Failed to create table: %s", err)
	}

	dbAllInstJson.Close()

}

func Test2_database_load_json(t *testing.T) {
	fakeDirectory := "./testdatafile"
	directoryJson := "./testdata_json/"
	DirParmission = 0777

	_, err := LoadDatabaseList(fakeDirectory, "json")
	if err == nil {
		t.Errorf("1.Failed to check directory")
	}
	if err != ErrDatabaseNotExist {
		t.Errorf("2.Failed to check directory. Unexpected err:%s", err)
	}
	_, err = LoadDatabaseList(directoryJson, "nojson")
	if err == nil {
		t.Errorf("3.Failed to check filetype")
	}
	if err != ErrInvalidFiletype {
		t.Errorf("4.Failed to check filetype. Unexpected err:%s", err)
	}
	dbAllInstJson, err := LoadDatabaseList(directoryJson, "json")
	if err != nil {
		t.Errorf("5.Failed to create new database list:%s", err)
	}
	_, err = dbAllInstJson.NewDatabase("database2")
	if err == nil || err == ErrNotImplemented {
		t.Errorf("8.Failed to refuse to create new database:%s", err)
	}

	dbInst1, err := dbAllInstJson.Get("database1")
	if err != nil {
		t.Errorf("9.Failed to get database:%s", err)
	}

	columnSet := []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time", Size: 15},
	}

	table1, err := dbInst1.GetTable("table1")

	if err != nil {
		t.Errorf("Failed to get table: %s", err)
	}

	testRow := make(Row)
	testRow["intline"] = int64(100)
	testRow["floatline"] = 11.5
	testRow["strline"] = "aaaa"
	testRow["dateline"] = time.Now()

	num, err := table1.WriteRow(testRow)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 1 {
		t.Errorf("Failed to insert row at 0: %d", num)
	}
	_, err = dbInst1.NewTable("table1", "static", columnSet)
	if err == nil {
		t.Errorf("Failed to prevent creating same table")
	}
	dbAllInstJson.Close()

}
