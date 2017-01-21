package tinydatabase

import (
	//"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	//"fmt"
	"io"
	"io/ioutil"
	//"math"
	"os"
	"path"
	//"strconv"
	//"time"
)

type TableStatic struct {
	tablefile   *os.File
	fileVersion int64
	columnTypes []ColumnType
	columnBytes int64
}

/*
 NewTable func creates table file and config file.
 When files exist, returns error.
*/
func (self *TableStatic) NewTable(directory string, tablename string, columnTypes []ColumnType) error {
	directory = path.Clean(directory)
	directory = directory + "/"
	dCheck, err := os.Stat(directory)
	dCheck = dCheck
	if err != nil {
		return err
	}
	if dCheck.IsDir() == false {
		return errors.New("Directory does not exist.")
	}

	_, err = os.Stat(directory + tablename + ".config")
	if err == nil {
		return errors.New("Config file exists.")
	}
	_, err = os.Stat(directory + tablename + ".table")
	if err == nil {
		return errors.New("Table file exists.")
	}
	err = self.Close()
	if err != nil {
		return err
	}

	err = self.setColumns(columnTypes)
	if err != nil {
		return err
	}
	err = self.saveConfigFile(directory + tablename + ".config")
	if err != nil {
		return err
	}

	err = self.openTableFile(directory + tablename + ".table")
	if err != nil {
		return err
	}
	return nil
}

/*
 Open func opens table file and config file.
*/
func (self *TableStatic) Open(directory string, tablename string) error {
	err := self.Close()
	if err != nil {
		return err
	}
	directory = path.Clean(directory)
	directory = directory + "/"
	err = self.openConfigFile(directory + tablename + ".config")
	if err != nil {
		return err
	}
	err = self.openTableFile(directory + tablename + ".table")
	if err != nil {
		return err
	}
	return nil
}

func (self *TableStatic) Close() error {
	if self.tablefile == nil {
		return nil
	}
	err := self.tablefile.Close()
	if err != nil {
		return err
	}
	self.tablefile = nil
	return nil
}

/*
 WriteRow func writes row on table file.
*/
func (self *TableStatic) WriteRow(row Row) (int64, error) {
	rowNum, err := self.searchLastRowNum()
	if err != nil {
		return -1, err
	}
	targetOff := self.convertRowNumToOffset(rowNum)
	var b []byte
	b = make([]byte, 1)
	b[0] = ROW_NORMAL
	num, err := self.tablefile.WriteAt(b, targetOff)
	if err != nil {
		return -1, err
	}
	targetOff = targetOff + int64(num)
	for _, v := range self.columnTypes {
		if val, ok := row[v.Name]; ok {
			b, err = v.ConvertToBytes(val)
			if err != nil {
				return -1, err
			}
		} else {
			b, err = v.GetNil()
			if err != nil {
				return -1, err
			}
		}
		num, err := self.tablefile.WriteAt(b, targetOff)
		if err != nil {
			return -1, err
		}
		targetOff = targetOff + int64(num)
	}
	err = self.tablefile.Sync()
	if err != nil {
		return -1, err
	}
	return rowNum, nil
}

func (self *TableStatic) ReadRow(rowNum int64) (Row, error) {
	lastRowNum, err := self.searchLastRowNum()
	if err != nil {
		return nil, err
	}
	if rowNum > lastRowNum {
		return nil, errors.New("Out of Row index")
	}
	if rowNum < 0 {
		return nil, errors.New("Out of Row index")
	}
	targetOff := self.convertRowNumToOffset(rowNum)

	var b []byte
	b = make([]byte, 1)
	_, err = self.tablefile.ReadAt(b, targetOff)
	if err != nil {
		return nil, err
	}
	if b[0] == ROW_DELETED {
		return nil, errors.New("Deleted row")
	}
	targetOff = targetOff + 1

	result := make(Row)
	for _, v := range self.columnTypes {
		b, err = v.GetNil()
		if err != nil {
			return nil, err
		}
		_, err = self.tablefile.ReadAt(b, targetOff)
		if err != nil {
			return nil, err
		}
		result[v.Name], err = v.ConvertToVal(b)
		if err != nil {
			return nil, err
		}
		nextOff, err := v.GetBytes()
		if err != nil {
			return nil, err
		}
		targetOff = targetOff + int64(nextOff)
	}
	return result, nil
}

func (self *TableStatic) DeleteRow(rowNum int64) error {
	lastRowNum, err := self.searchLastRowNum()
	if err != nil {
		return err
	}
	if rowNum > lastRowNum {
		return errors.New("Out of Row index")
	}
	if rowNum < 0 {
		return errors.New("Out of Row index")
	}

	targetOff := self.convertRowNumToOffset(rowNum)
	var b []byte
	b = make([]byte, 1)
	b[0] = ROW_DELETED
	_, err = self.tablefile.WriteAt(b, targetOff)
	err = self.tablefile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (self *TableStatic) GetTableType() string {
	return "TableStatic"
}

//**************************************************

func (self *TableStatic) openConfigFile(configfilename string) error {

	jsonString, err := ioutil.ReadFile(configfilename)
	if err != nil {
		return err
	}

	var columnTypes []ColumnType
	err = json.Unmarshal(jsonString, &columnTypes)
	if err != nil {
		return err
	}
	err = self.setColumns(columnTypes)
	if err != nil {
		return err
	}

	return nil
}
func (self *TableStatic) openTableFile(tablefilename string) error {
	f, err := os.OpenFile(tablefilename, os.O_RDWR+os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	self.tablefile = f

	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.tablefile.ReadAt(b, 0)
	if err != nil {
		if err == io.EOF {
			self.fileVersion = STATIC1
			binary.PutVarint(b, self.fileVersion)
			num, err = self.tablefile.WriteAt(b, 0)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		self.fileVersion, num = binary.Varint(b)
		if num == 0 {
			return errors.New("Failed to read fileversion")
		}
		if self.fileVersion != STATIC1 {
			return errors.New("Fileversion is not correct")
		}
	}

	return err
}

func (self *TableStatic) saveConfigFile(configfile string) error {
	b, err := json.Marshal(self.columnTypes)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(configfile, b, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func (self *TableStatic) setColumns(columnTypes []ColumnType) error {
	columnBytes := int64(0)
	flags := map[string]int{}

	for _, val := range columnTypes {
		_, ok := flags[val.Name]
		if ok == true {
			return errors.New("Same column name exists.")
		}
		flags[val.Name] = 1
		num, err := val.GetBytes()
		if err != nil {
			return err
		}
		val.Size = num
		columnBytes = columnBytes + num
	}
	self.columnTypes = columnTypes
	self.columnBytes = columnBytes
	return nil
}

func (self *TableStatic) convertRowNumToOffset(rowNum int64) int64 {
	offset := int64(rowNum)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64)
	return offset
}
func (self *TableStatic) convertOffsetToRowNum(offset int64) int64 {
	rowNum := int64((offset - int64(binary.MaxVarintLen64)) / (int64(self.columnBytes + 1)))
	return rowNum
}

func (self *TableStatic) searchLastRowNum() (int64, error) {
	lastOff, err := self.tablefile.Seek(0, 2)
	if err != nil {
		return -1, err
	}
	rowNum := self.convertOffsetToRowNum(lastOff)
	return rowNum, err
}

/*func fileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (self TinyDatabaseError) Error() string {
	return self.Message
}*/
