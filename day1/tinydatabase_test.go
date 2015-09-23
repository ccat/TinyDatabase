package tinydatabase

import (
	"os"
	"testing"
	"time"
)

func Test1_basicUsage(t *testing.T) {
	//dir, _ := os.Getwd()//dir+
	tableFileName := "test.table"
	os.Remove(tableFileName)

	columnSet := []ColumnType{
		{Name: "intline", Type: "int64"},
		{Name: "floatline", Type: "float64"},
		{Name: "strline", Type: "string256"},
		{Name: "dateline", Type: "time"},
	}

	tableInst, err := NewTable(tableFileName, columnSet)

	if err != nil {
		t.Errorf("Failed to create table: %s", err)
	}
	if tableInst == nil {
		t.Errorf("Failed to create table: tableInst==nil")
	}
	_, err = os.Stat(tableFileName)
	if err != nil {
		t.Errorf("Failed to create table file:%s", err)
	}

	testMap := make(map[string]interface{})
	testMap["intline"] = int64(100)
	testMap["floatline"] = 10.5
	testMap["strline"] = "aaaa"
	testMap["dateline"] = time.Now()

	num, err := tableInst.Insert(testMap)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 0 {
		t.Errorf("Failed to insert row at 0")
	}
	num, err = tableInst.Insert(testMap)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 1 {
		t.Errorf("Failed to insert row at 1")
	}
	num = 0

	testMap2, err := tableInst.Read(num)
	if err != nil {
		t.Errorf("Failed to read row at 0: %s", err)
	}
	if testMap2["intline"] != testMap["intline"] {
		t.Errorf("Failed to read row at 0: intline")
	}
	if testMap2["floatline"] != testMap["floatline"] {
		t.Errorf("Failed to read row at 0: floatline")
	}
	str1, _ := testMap["strline"].(string)
	str2, _ := testMap2["strline"].(string)
	if str2 != str1 {
		t.Errorf("Failed to read row at 0: strline: %s!=%s", str2, str1)
		t.Errorf("len testMap:%d", len(str1))
		t.Errorf("len testMap2:%d", len(str2))
	}
	if testMap2["dateline"] != testMap["dateline"] {
		t.Errorf("Failed to read row at 0: dateline")
	}

	testMap2["intline"] = int64(102)
	err = tableInst.Update(num, testMap2)
	if err != nil {
		t.Errorf("Failed to update row at 0: %s", err)
	}
	testMap3, err := tableInst.Read(num)
	if testMap2["intline"] != testMap3["intline"] {
		t.Errorf("Failed to update row at 0: intline")
	}

	err = tableInst.Delete(num)
	if err != nil {
		t.Errorf("Failed to delete row at 0: %s", err)
	}
	_, err = tableInst.Read(num)
	if err.Error() != "Deleted row" {
		t.Errorf("Failed to delete row at 0: %s", err)
	}

	tableInst.Close()

}

func Test2_errUsage(t *testing.T) {
	columnSet := []ColumnType{
		{Name: "intline", Type: "int64"},
		{Name: "floatline", Type: "float64"},
		{Name: "strline", Type: "string256"},
		{Name: "dateline", Type: "time2"},
	}

	dir, _ := os.Getwd()
	tableInst, err := NewTable(dir+"test.table", columnSet)

	if err == nil {
		t.Errorf("Failed to return column type error")
	}
	if tableInst != nil {
		t.Errorf("Failed to stop create table")
	}

}
