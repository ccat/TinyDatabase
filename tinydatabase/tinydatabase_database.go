/*
 Package tinydatabase provides simple database functions.
*/
package tinydatabase

import (
	//"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strings"
)

//Database is a manager struct of tables.
type Database struct {
	filetype  string
	directory string
	tables    map[string]TableInterface
}

//DatabaseList is a manager struct of databases.
type DatabaseList struct {
	filetype  string
	directory string
	Databases map[string]*Database
}

var (
	ErrNotDir           = errors.New("Specified path is not directory")
	ErrDatabaseExist    = errors.New("Specified directory contains databases")
	ErrInvalidFiletype  = errors.New("Specified file type is invalid")
	ErrDatabaseNotExist = errors.New("Specified database is not existed")
	ErrInvalidTabletype = errors.New("Specified table type is invalid")
	ErrTableNotExist    = errors.New("Specified table is not existed")
	ErrNotImplemented   = errors.New("Not Implemented")
	DirParmission       = 0755
)

//NewDatabaseList creates a new DatabaseList with directory.
func NewDatabaseList(directory string, databaseType string) (result *DatabaseList, err error) {
	if databaseType != "json" /*&& databaseType != "toml"*/ {
		return nil, ErrInvalidFiletype
	}
	err = createDir(directory)
	if err != nil {
		return nil, err
	}
	directory = strings.TrimSuffix(directory, "/")
	err = dbExistanceCheck(directory + "/databases.config")
	if err != nil {
		return nil, err
	}

	result = &DatabaseList{}
	result.directory = directory
	result.filetype = databaseType
	result.Databases = map[string]*Database{}
	err = result.Save()
	return result, err
}

//LoadDatabaseList loads DatabaseList from directory.
func LoadDatabaseList(directory string, databaseType string) (result *DatabaseList, err error) {
	if databaseType != "json" /*&& databaseType != "toml"*/ {
		return nil, ErrInvalidFiletype
	}
	directory = strings.TrimSuffix(directory, "/")
	err = dbExistanceCheck(directory + "/databases.config")
	if err != ErrDatabaseExist {
		return nil, ErrDatabaseNotExist
	}

	result = &DatabaseList{}
	result.directory = directory
	result.filetype = databaseType
	result.Databases = map[string]*Database{}
	err = result.Load()
	return result, err
}

//NewDatabase creates new Database under the DatabaseList directory.
func (self *DatabaseList) NewDatabase(name string) (result *Database, err error) {
	_, ok := self.Databases[name]
	if ok == true {
		return nil, ErrDatabaseExist
	}
	self.Databases[name] = &Database{}
	err = self.Databases[name].New(self.directory+"/"+name, self.filetype)
	if err != nil {
		return nil, err
	}
	err = self.Save()
	return self.Databases[name], err
}

//Get returns specified Database or error.
func (self *DatabaseList) Get(name string) (result *Database, err error) {
	result, ok := self.Databases[name]
	if ok == false {
		return nil, ErrDatabaseNotExist
	}
	return result, nil
}

//Save saves all databases and tables.
func (self *DatabaseList) Save() (err error) {
	file, err := os.Create(self.directory + "/databases.config")
	if err != nil {
		return err
	}
	defer file.Close()
	dbNameList := []string{}
	for key, val := range self.Databases {
		dbNameList = append(dbNameList, key)
		err = val.Save()
		if err != nil {
			return err
		}
	}
	if self.filetype == "json" {
		byteS, err := json.Marshal(dbNameList)
		if err != nil {
			return err
		}
		num := 0
		for num < len(byteS) {
			tempnum, err := file.Write(byteS[num:])
			if err != nil {
				return err
			}
			num += tempnum
		}
	} /*else if self.filetype == "toml" {
		buf := new(bytes.Buffer)
		err = toml.NewEncoder(buf).Encode(dbNameList)
		if err != nil {
			return err
		}
		byteS := buf.Bytes()
		num := 0
		for num < len(byteS) {
			tempnum, err := file.Write(byteS[num:])
			if err != nil {
				return err
			}
			num += tempnum
		}
	}*/
	return nil
}

//Load loads DatabaseList.
func (self *DatabaseList) Load() (err error) {
	data, err := ioutil.ReadFile(self.directory + "/databases.config")
	if err != nil {
		return err
	}
	dbNameList := []string{}
	if self.filetype == "json" {
		err = json.Unmarshal(data, &dbNameList)
		if err != nil {
			return err
		}
	} /*else if self.filetype == "toml" {
		_, err = toml.Decode(string(data), &dbNameList)
		return ErrNotImplemented
	}*/
	for i := 0; i < len(dbNameList); i++ {
		self.Databases[dbNameList[i]] = &Database{}
		err = self.Databases[dbNameList[i]].Load(self.directory+"/"+dbNameList[i], self.filetype)
		if err != nil {
			return err
		}
	}
	return nil
}

//Close closes all Databases.
func (self *DatabaseList) Close() (err error) {
	for _, val := range self.Databases {
		err = val.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

//New creates Database on the directory.
func (self *Database) New(directory string, filetype string) error {
	if filetype != "json" /*&& filetype != "toml"*/ {
		return ErrInvalidFiletype
	}

	err := createDir(directory)
	if err != nil {
		return err
	}
	err = dbExistanceCheck(directory + "/tables.config")
	if err != nil {
		return err
	}
	self.directory = directory
	self.filetype = filetype
	self.tables = map[string]TableInterface{}
	err = self.Save()

	return err
}

//Save saves config and all tables.
func (self *Database) Save() error {
	file, err := os.Create(self.directory + "/tables.config")
	if err != nil {
		return err
	}
	defer file.Close()
	tableNameMap := map[string]string{}
	for key, val := range self.tables {
		//tableNameList = append(tableNameList, key)
		tableNameMap[key] = val.GetTableType()
	}
	if self.filetype == "json" {
		bytes, err := json.Marshal(tableNameMap)
		if err != nil {
			return err
		}
		num := 0
		for num < len(bytes) {
			tempnum, err := file.Write(bytes[num:])
			if err != nil {
				return err
			}
			num += tempnum
		}
	} /*else if self.filetype == "toml" {
		return ErrNotImplemented
	}*/
	return nil
}

//Load loads Database from directory.
func (self *Database) Load(directory string, filetype string) error {
	if filetype != "json" /*&& filetype != "toml"*/ {
		return ErrInvalidFiletype
	}
	err := dbExistanceCheck(directory + "/tables.config")
	if err != ErrDatabaseExist {
		return ErrDatabaseNotExist
	}
	self.directory = directory
	self.filetype = filetype
	self.tables = map[string]TableInterface{}

	data, err := ioutil.ReadFile(self.directory + "/tables.config")
	if err != nil {
		return err
	}
	tableNameMap := map[string]string{}
	if self.filetype == "json" {
		err = json.Unmarshal(data, &tableNameMap)
	} /* else if self.filetype == "toml" {
		return ErrNotImplemented
	}*/
	if err != nil {
		return err
	}
	for key, val := range tableNameMap {
		var tableI TableInterface
		if val == "TableStatic" {
			tableI = &TableStatic{}

		} else if val == "TableDynamic" {
			tableI = &TableDynamic{}
		} else {
			return ErrNotImplemented
		}
		err = tableI.Open(self.directory, key)
		if err != nil {
			return err
		}
		self.tables[key] = tableI
	}

	return err
}

//NewTable creates table.
func (self *Database) NewTable(tablename string, tableType string, columnTypes []ColumnType) (result TableInterface, err error) {
	if tableType == "static" {
		result = &TableStatic{}
	} else if tableType == "dynamic" {
		result = &TableDynamic{}
	} else {
		return nil, ErrInvalidTabletype
	}

	err = result.NewTable(self.directory, tablename, columnTypes)
	if err != nil {
		return nil, err
	}
	self.tables[tablename] = result
	err = self.Save()
	return result, err
}

//GetTable returns table.
func (self *Database) GetTable(name string) (TableInterface, error) {
	result, ok := self.tables[name]
	if ok == false {
		return nil, ErrTableNotExist
	}
	return result, nil
}

//Close closes tables.
func (self *Database) Close() (err error) {
	for _, val := range self.tables {
		err = val.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

//createDir create directory when not exist.
func createDir(directory string) error {
	fInfo, err := os.Stat(directory)
	if err != nil {
		if os.IsExist(err) == false {
			err = os.Mkdir(directory, os.FileMode(DirParmission))
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		if fInfo.IsDir() == false {
			return ErrNotDir
		}
	}
	return nil
}

//dbExistanceCheck checks database is there or not.
func dbExistanceCheck(filePath string) error {
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsExist(err) == true {
			return ErrDatabaseExist
		}
	} else {
		return ErrDatabaseExist
	}
	return nil
}
