package tinydatabase

import (
	//"fmt"
	"os"
	//"path"
	"strings"
	"testing"
	"time"
)

func Test1_TableDynamic_basicUsage(t *testing.T) {
	directory := "./"
	tablename := "test"
	os.Remove(directory + tablename + ".table")
	os.Remove(directory + tablename + ".index")
	os.Remove(directory + tablename + ".config")
	columnSet := []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time", Size: 15},
	}

	var tableInst TableInterface
	tableInst = &TableDynamic{}
	err := tableInst.NewTable(directory, tablename, columnSet)

	if err != nil {
		t.Errorf("Failed to create table: %s", err)
	}
	_, err = os.Stat(tablename + ".table")
	if err != nil {
		t.Errorf("Failed to create table file:%s", err)
	}
	_, err = os.Stat(tablename + ".index")
	if err != nil {
		t.Errorf("Failed to create table file:%s", err)
	}
	_, err = os.Stat(tablename + ".config")
	if err != nil {
		t.Errorf("Failed to create config file:%s", err)
	}
	err = tableInst.Open(directory, tablename)

	if err != nil {
		t.Errorf("Failed to open table: %s", err)
	}

	testRow := make(Row)
	testRow["intline"] = int64(100)
	testRow["floatline"] = 10.5
	testRow["strline"] = "aaaa"
	testRow["dateline"] = time.Now()

	num, err := tableInst.WriteRow(-1, testRow)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 0 {
		t.Errorf("Failed to insert row at 0: %d", num)
	}

	testRow["floatline"] = 12.5
	num, err = tableInst.WriteRow(-1, testRow)
	if err != nil {
		t.Errorf("Failed to insert row: %s", err)
	}
	if num != 1 {
		t.Errorf("Failed to insert row at 1:%d", num)
	}

	num = 0
	testRow["floatline"] = 10.5
	testRow2, err := tableInst.ReadRow(num)
	if err != nil {
		t.Errorf("Failed to read row at 0: %s", err)
	}
	if testRow2["intline"] != testRow["intline"] {
		t.Errorf("Failed to read row at 0: intline")
	}
	if testRow2["floatline"] != testRow["floatline"] {
		t.Errorf("Failed to read row at 0: floatline")
	}
	str1, _ := testRow["strline"].(string)
	str2, _ := testRow2["strline"].(string)
	if str2 != str1 {
		t.Errorf("Failed to read row at 0: strline: %s!=%s", str2, str1)
		t.Errorf("len testMap:%d", len(str1))
		t.Errorf("len testMap2:%d", len(str2))
	}
	if testRow2["dateline"] != testRow["dateline"] {
		t.Errorf("Failed to read row at 0: dateline")
	}

	testRow2["intline"] = int64(102)
	_, err = tableInst.WriteRow(num, testRow2)
	if err != nil {
		t.Errorf("Failed to update row at 0: %s", err)
	}
	testRow3, err := tableInst.ReadRow(num)
	if testRow2["intline"] != testRow3["intline"] {
		t.Errorf("Failed to update row at 0: intline")
	}

	err = tableInst.DeleteRow(num)
	if err != nil {
		t.Errorf("Failed to delete row at 0: %s", err)
	}
	_, err = tableInst.ReadRow(num)
	if err.Error() != "Deleted row" {
		t.Errorf("Failed to delete row at 0: %s", err)
	}

	testRow2["intline"] = "string data"
	_, err = tableInst.WriteRow(-1, testRow2)
	if err == nil {
		t.Errorf("Failed to check invalid data")
	}
	if err != nil && strings.HasPrefix(err.Error(), "Missmatch type(int64)") == false {
		t.Errorf("Failed to check invalid data: %s", err)
	}

	testRow2["intline"] = 100
	_, err = tableInst.WriteRow(-1, testRow2)
	if err != nil {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	testRow2["intline"] = int32(100)
	_, err = tableInst.WriteRow(-1, testRow2)
	if err != nil {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	testRow2["intline"] = int16(100)
	_, err = tableInst.WriteRow(-1, testRow2)
	if err != nil {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	testRow2["intline"] = int8(100)
	_, err = tableInst.WriteRow(-1, testRow2)
	if err != nil {
		t.Errorf("Failed to check invalid data: %s", err)
	}

	testRow2["intline"] = 100.2
	_, err = tableInst.WriteRow(-1, testRow2)
	if err == nil {
		t.Errorf("Failed to check invalid data")
	}
	if err != nil && strings.HasPrefix(err.Error(), "Missmatch type(int64)") == false {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	testRow2["intline"] = int64(100)
	testRow2["strline"] = 100
	_, err = tableInst.WriteRow(-1, testRow2)
	if err == nil {
		t.Errorf("Failed to check invalid data")
	}
	if err != nil && strings.HasPrefix(err.Error(), "Missmatch type(string)") == false {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	testRow2["strline"] = 100.2
	_, err = tableInst.WriteRow(-1, testRow2)
	if err == nil {
		t.Errorf("Failed to check invalid data")
	}
	if err != nil && strings.HasPrefix(err.Error(), "Missmatch type(string)") == false {
		t.Errorf("Failed to check invalid data: %s", err)
	}

	testRow2["strline"] = "test"
	num, err = tableInst.WriteRow(1000, testRow2)
	if err != nil {
		t.Errorf("Failed to check invalid data: %s", err)
	}
	if num == 1000 {
		t.Errorf("Failed to change row num")
	}

	_, err = tableInst.ReadRow(1000)
	if err == nil {
		t.Errorf("Failed to raise error for invalid row")
	}

	err = tableInst.DeleteRow(1000)
	if err == nil {
		t.Errorf("Failed to raise error for invalid row")
	}

	_, err = tableInst.ReadRow(-1)
	if err == nil {
		t.Errorf("Failed to raise error for invalid row")
	}

	err = tableInst.DeleteRow(-1)
	if err == nil {
		t.Errorf("Failed to raise error for invalid row")
	}

	tableInst.Close()

}

func Test2_TableDynamic_errUsage(t *testing.T) {
	directory := "./"
	tablename := "test"
	os.Remove(tablename + ".table")
	os.Remove(tablename + ".config")

	columnSet := []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time2", Size: 15},
	}

	var tableInst TableInterface
	tableInst = &TableDynamic{}
	err := tableInst.NewTable(directory, tablename, columnSet)

	if err == nil {
		t.Errorf("Failed to return column type error")
	}

	columnSet = []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time", Size: 15},
		{Name: "strline", Type: "string", Size: 256},
	}

	err = tableInst.NewTable(directory, tablename, columnSet)

	if err == nil {
		t.Errorf("Failed to return column config error")
	}

	directory = "./notexistdir"
	columnSet = []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 10},
		{Name: "dateline", Type: "time", Size: 15},
	}

	err = tableInst.NewTable(directory, tablename, columnSet)

	if err == nil {
		t.Errorf("Failed to return directory check error")
	}

	directory = "./"
	columnSet = []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: -1},
		{Name: "dateline", Type: "time", Size: 15},
	}

	err = tableInst.NewTable(directory, tablename, columnSet)

	if err == nil {
		t.Errorf("Failed to return size check error")
	}

	columnSet = []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 10},
		{Name: "dateline", Type: "time", Size: 15},
	}

	err = tableInst.NewTable(directory, tablename, columnSet)
	if err != nil {
		t.Errorf("Failed to create new table:%s", err)
	}

	testRow := make(Row)
	testRow["intline"] = int64(100)
	testRow["floatline"] = 10.5
	testRow["strline"] = "This is over than 10 words"
	testRow["dateline"] = time.Now()

	_, err = tableInst.WriteRow(-1, testRow)
	if err == nil {
		t.Errorf("Failed to check string count")
	}

	err = tableInst.NewTable(directory, tablename, columnSet)
	if err == nil {
		t.Errorf("Failed to check file overwrite")
	}

	tableInst.Close()

}
