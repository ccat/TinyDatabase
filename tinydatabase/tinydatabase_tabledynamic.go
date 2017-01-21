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

type TableDynamic struct {
	tablefile           *os.File
	indexfile           *os.File
	fileVersion         int64
	columnTypes         []ColumnType
	columnBytes         int64
	numOfFlexibleColumn int64
}

/*
 NewTable func creates table file and config file.
 When files exist, returns error.
*/
func (self *TableDynamic) NewTable(directory string, tablename string, columnTypes []ColumnType) error {
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
	_, err = os.Stat(directory + tablename + ".index")
	if err == nil {
		return errors.New("Index file exists.")
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
	err = self.openIndexFile(directory + tablename + ".index")
	if err != nil {
		return err
	}
	return nil
}

/*
 Open func opens table file and config file.
*/
func (self *TableDynamic) Open(directory string, tablename string) error {
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
	err = self.openIndexFile(directory + tablename + ".index")
	if err != nil {
		return err
	}
	return nil
}

func (self *TableDynamic) Close() error {
	if self.tablefile != nil {
		err := self.tablefile.Close()
		if err != nil {
			return err
		}
		self.tablefile = nil
	}
	if self.indexfile != nil {
		err := self.indexfile.Close()
		if err != nil {
			return err
		}
		self.indexfile = nil
	}
	return nil
}

/*
 WriteRow func writes row on table file.
*/
func (self *TableDynamic) WriteRow(row Row) (int64, error) {
	tableOff, err := self.searchLastTableOffset()
	if err != nil {
		return -1, err
	}
	indexNum, err := self.searchLastIndexNum()
	if err != nil {
		return -1, err
	}
	indexOff := self.convertIndexNumToOffset(indexNum)

	var b []byte
	b = make([]byte, 1)
	b[0] = ROW_NORMAL
	num, err := self.tablefile.WriteAt(b, tableOff)
	if err != nil {
		return -1, err
	}

	b = make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(b, tableOff)
	num2, err := self.indexfile.WriteAt(b, indexOff)
	if err != nil {
		return -1, err
	}

	tableOff = tableOff + int64(num)
	indexOff = indexOff + int64(num2)
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
		num, err = self.tablefile.WriteAt(b, tableOff)
		if err != nil {
			return -1, err
		}
		if v.Size == 0 {
			b = make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(b, int64(num))
			num2, err := self.indexfile.WriteAt(b, indexOff)
			if err != nil {
				return -1, err
			}
			indexOff = indexOff + int64(num2)
		}
		tableOff = tableOff + int64(num)
	}
	b = make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(b, tableOff)
	_, err = self.indexfile.WriteAt(b, int64(binary.MaxVarintLen64))
	if err != nil {
		return -1, err
	}

	err = self.tablefile.Sync()
	if err != nil {
		return -1, err
	}
	err = self.indexfile.Sync()
	if err != nil {
		return -1, err
	}
	return indexNum, nil
}

func (self *TableDynamic) ReadRow(rowNum int64) (Row, error) {
	lastIndexNum, err := self.searchLastIndexNum()
	if err != nil {
		return nil, err
	}
	if rowNum > lastIndexNum {
		return nil, errors.New("Out of Row index")
	}
	if rowNum < 0 {
		return nil, errors.New("Out of Row index")
	}
	indexOff := self.convertIndexNumToOffset(rowNum)

	var b []byte
	b = make([]byte, binary.MaxVarintLen64)
	num, err := self.indexfile.ReadAt(b, indexOff)
	if err != nil {
		return nil, err
	}
	if num != binary.MaxVarintLen64 {
		return nil, errors.New("Failed to read table index")
	}
	tableOff, num := binary.Varint(b)
	if num < 1 {
		return nil, errors.New("Failed to read table index")
	}
	indexOff = indexOff + int64(binary.MaxVarintLen64)

	b = make([]byte, 1)
	_, err = self.tablefile.ReadAt(b, tableOff)
	if err != nil {
		return nil, err
	}
	if b[0] == ROW_DELETED {
		return nil, errors.New("Deleted row")
	}
	tableOff = tableOff + 1

	result := make(Row)
	for _, v := range self.columnTypes {
		size, err := v.GetBytes()
		if err != nil {
			return nil, err
		}
		b, err = v.GetNil()
		if err != nil {
			return nil, err
		}
		if size > 0 {
			num, err = self.tablefile.ReadAt(b, tableOff)
			if err != nil {
				return nil, err
			}
			result[v.Name], err = v.ConvertToVal(b)
			if err != nil {
				return nil, err
			}
			tableOff += int64(num)
		} else {
			b = make([]byte, binary.MaxVarintLen64)
			num, err = self.indexfile.ReadAt(b, indexOff)
			indexOff += int64(num)
			size, num = binary.Varint(b)
			if num < 1 {
				return nil, errors.New("Failed to read index")
			}
			b = make([]byte, size)

			num, err = self.tablefile.ReadAt(b, tableOff)
			if err != nil {
				return nil, err
			}
			result[v.Name], err = v.ConvertToVal(b)
			if err != nil {
				return nil, err
			}
			tableOff += int64(num)

		}
	}
	return result, nil
}

func (self *TableDynamic) DeleteRow(rowNum int64) error {
	lastIndexNum, err := self.searchLastIndexNum()
	if err != nil {
		return err
	}
	if rowNum > lastIndexNum {
		return errors.New("Out of Row index")
	}
	if rowNum < 0 {
		return errors.New("Out of Row index")
	}
	indexOff := self.convertIndexNumToOffset(rowNum)

	var b []byte
	b = make([]byte, binary.MaxVarintLen64)
	_, err = self.indexfile.ReadAt(b, indexOff)
	if err != nil {
		return err
	}
	tableOff, num := binary.Varint(b)
	if num == 0 {
		return errors.New("Failed to read table index")
	}
	b = make([]byte, 1)
	b[0] = ROW_DELETED
	_, err = self.tablefile.WriteAt(b, tableOff)
	err = self.tablefile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (self *TableDynamic) GetTableType() string {
	return "TableDynamic"
}

//**************************************************

func (self *TableDynamic) openConfigFile(configfilename string) error {

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

func (self *TableDynamic) openTableFile(tablefilename string) error {
	f, err := os.OpenFile(tablefilename, os.O_RDWR+os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	self.tablefile = f

	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.tablefile.ReadAt(b, 0)
	if err != nil {
		if err == io.EOF {
			self.fileVersion = DYNAMIC1_TABLE
			binary.PutVarint(b, self.fileVersion)
			num, err = self.tablefile.WriteAt(b, 0)
			if err != nil {
				return err
			}
			err = self.tablefile.Sync()
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
		if self.fileVersion != DYNAMIC1_TABLE {
			return errors.New("Fileversion is not correct")
		}
	}

	return err
}

func (self *TableDynamic) openIndexFile(indexfilename string) error {
	f, err := os.OpenFile(indexfilename, os.O_RDWR+os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	self.indexfile = f

	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.indexfile.ReadAt(b, 0)
	if err != nil {
		if err == io.EOF {
			binary.PutVarint(b, DYNAMIC1_INDEX)
			num, err = self.indexfile.WriteAt(b, 0)
			if err != nil {
				return err
			}
			binary.PutVarint(b, int64(binary.MaxVarintLen64))
			num, err = self.indexfile.WriteAt(b, int64(num))
			if err != nil {
				return err
			}
			err = self.indexfile.Sync()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		v, num := binary.Varint(b)
		if num == 0 {
			return errors.New("Failed to read fileversion")
		}
		if v != DYNAMIC1_INDEX {
			return errors.New("Fileversion is not correct")
		}
	}

	return err
}

func (self *TableDynamic) saveConfigFile(configfile string) error {
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

func (self *TableDynamic) setColumns(columnTypes []ColumnType) error {
	columnBytes := int64(0)
	numOfFlexibleColumn := int64(0)
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
		if num == 0 {
			numOfFlexibleColumn += 1
		}
	}
	self.columnTypes = columnTypes
	self.columnBytes = columnBytes
	self.numOfFlexibleColumn = numOfFlexibleColumn
	return nil
}

func (self *TableDynamic) convertIndexNumToOffset(indexNum int64) int64 {
	offset := indexNum*(int64(binary.MaxVarintLen64)*(self.numOfFlexibleColumn+1)) + int64(binary.MaxVarintLen64)*2
	return offset
}
func (self *TableDynamic) convertOffsetToIndexNum(offset int64) int64 {
	indexNum := int64((offset - int64(binary.MaxVarintLen64)*2) / (int64(binary.MaxVarintLen64) * (self.numOfFlexibleColumn + 1)))
	return indexNum
}

func (self *TableDynamic) searchLastIndexNum() (int64, error) {
	lastOff, err := self.indexfile.Seek(0, 2)
	if err != nil {
		return -1, err
	}
	indexNum := self.convertOffsetToIndexNum(lastOff)
	return indexNum, err
}

func (self *TableDynamic) searchLastTableOffset() (int64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.indexfile.ReadAt(b, int64(binary.MaxVarintLen64))
	if err != nil {
		return -1, err
	}
	v, num := binary.Varint(b)
	if num == 0 {
		return -1, errors.New("Failed to read last table offset")
	}
	return v, nil

}
