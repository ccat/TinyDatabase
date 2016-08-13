package tinydatabase

import (
	"os"
	"testing"
	"time"
)

func Test1_basicUsage(t *testing.T) {
	//dir, _ := os.Getwd()//dir+
	tableFileName := "test.table"
	configFileName := "test.config"
	os.Remove(tableFileName)
	os.Remove(configFileName)

	columnSet := []ColumnType{
		{Name: "intline", Type: "int64", Size: 64},
		{Name: "floatline", Type: "float64", Size: 64},
		{Name: "strline", Type: "string", Size: 256},
		{Name: "dateline", Type: "time", Size: 15},
	}

	//tableInst, err := NewTable(tableFileName, columnSet)
	var tableInst Table
	err := tableInst.NewTable(configFileName, tableFileName, columnSet)

	if err != nil {
		t.Errorf("Failed to create table: %s", err)
	}
	_, err = os.Stat(tableFileName)
	if err != nil {
		t.Errorf("Failed to create table file:%s", err)
	}
	_, err = os.Stat(configFileName)
	if err != nil {
		t.Errorf("Failed to create config file:%s", err)
	}
	err = tableInst.Open(configFileName, tableFileName)

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
	tableInst.Close()

}

/*func Test2_errUsage(t *testing.T) {
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

}*/
