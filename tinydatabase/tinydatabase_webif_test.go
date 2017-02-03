package tinydatabase

import (
	"testing"
)

func Test1_WebifFuncs_basicUsage(t *testing.T) {
	directoryJson := "./testdata_json/"
	//tablename := "testdynamic"
	DirParmission = 0777
	os.RemoveAll(directoryJson)
	//os.Mkdir(directoryJson, 0777)

	w := WebIF{}
	w.Databases, err = NewDatabaseList(directoryJson, "json")
	if err != nil {
		t.Errorf("Failed to create new database list:%s", err)
	}
}
