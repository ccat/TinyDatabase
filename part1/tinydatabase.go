package tinydatabase

import (
	//"fmt"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"time"
)

type ColumnType struct {
	Name string
	Type string
	Size int64
}

type Row map[string]interface{}

type TableInterface interface {
	NewTable(configfile string, tablefile string, columnTypes []ColumnType) error
	Open(configfile string, tablefile string) error
	Close() error
	ReadRow(rowNum int64) (Row, error)
	WriteRow(rowNum int64, row Row) (int64, error)
	DeleteRow(rowNum int64) error
}

type Table struct {
	tablefile   *os.File
	fileVersion int64
	columnTypes []ColumnType
	columnBytes int64
}

func (self *Table) NewTable(configfile string, tablefile string, columnTypes []ColumnType) error {
	err := self.setColumns(columnTypes)
	if err != nil {
		return err
	}
	err = self.saveConfigFile(configfile)
	if err != nil {
		return err
	}

	err = self.openTableFile(tablefile)
	if err != nil {
		return err
	}
	return nil
}

func (self *Table) Open(configfile string, tablefile string) error {
	err := self.Close()
	if err != nil {
		return err
	}
	err = self.openConfigFile(configfile)
	if err != nil {
		return err
	}
	err = self.openTableFile(tablefile)
	if err != nil {
		return err
	}
	return nil
}

func (self *Table) Close() error {
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

func (self *Table) WriteRow(rowNum int64, row Row) (int64, error) {
	if rowNum == -1 {
		temp, err := self.searchLastOff()
		if err != nil {
			return -1, err
		}
		rowNum = temp
	}
	targetOff := self.convertRowNumToOffset(rowNum)
	var b []byte
	b = make([]byte, 1)
	b[0] = 1
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

func (self *Table) ReadRow(rowNum int64) (Row, error) {
	targetOff := self.convertRowNumToOffset(rowNum)

	var b []byte
	b = make([]byte, 1)
	_, err := self.tablefile.ReadAt(b, targetOff)
	if err != nil {
		return nil, err
	}
	if b[0] == 0 {
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

func (self *Table) DeleteRow(rowNum int64) error {
	targetOff := self.convertRowNumToOffset(rowNum)
	var b []byte
	b = make([]byte, 1)
	b[0] = 0
	_, err := self.tablefile.WriteAt(b, targetOff)
	err = self.tablefile.Sync()
	if err != nil {
		return err
	}
	return nil
}

//**************************************************

func (self *ColumnType) GetBytes() (int64, error) {
	if self.Type == "int64" {
		return binary.MaxVarintLen64, nil
	} else if self.Type == "float64" {
		return 8, nil
	} else if self.Type == "string" {
		return self.Size, nil
	} else if self.Type == "time" {
		return 15, nil
	}
	return 0, errors.New("Type is not valid")
}

func (self *ColumnType) GetNil() ([]byte, error) {
	var b []byte
	byteNum, err := self.GetBytes()
	if err != nil {
		return nil, err
	}
	b = make([]byte, byteNum)
	if self.Type == "int64" {
		binary.PutVarint(b, int64(0))
		return b, nil
	} else if self.Type == "float64" {
		bits := math.Float64bits(0.0)
		binary.LittleEndian.PutUint64(b, bits)
		return b, nil
	} else if self.Type == "string" {
		return b, nil
	} else if self.Type == "time" {
		b, err = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC).MarshalBinary()
		if err != nil {
			return nil, err
		}
		return b, nil
	} else {
		return nil, errors.New("Type is not valid")
	}
}

func (self *ColumnType) ConvertToBytes(val interface{}) ([]byte, error) {
	var b []byte
	byteNum, err := self.GetBytes()
	if err != nil {
		return nil, err
	}
	if self.Type == "int64" {
		v, ok := val.(int64)
		if ok == false {
			return nil, errors.New("Missmatch type(int64) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		binary.PutVarint(b, v)
		return b, nil
	} else if self.Type == "float64" {
		v, ok := val.(float64)
		if ok == false {
			return nil, errors.New("Missmatch type(float64) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		bits := math.Float64bits(v)
		binary.LittleEndian.PutUint64(b, bits)
		return b, nil
	} else if self.Type == "string" {
		v, ok := val.(string)
		if ok == false {
			return nil, errors.New("Missmatch type(string) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		for i := 0; i < len(v); i++ {
			b[i] = v[i]
		}
		return b, nil
	} else if self.Type == "time" {
		v, ok := val.(time.Time)
		if ok == false {
			return nil, errors.New("Missmatch type(time) and val: " + self.Name)
		}
		b, err = v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return b, nil
	} else {
		return nil, errors.New("Type is not valid: " + self.Name)
	}
}

func (self *ColumnType) ConvertToVal(b []byte) (interface{}, error) {
	if self.Type == "int64" {
		var v int64
		v, num := binary.Varint(b)
		if num < 1 {
			return nil, errors.New("Missmatch type(int64) and val: " + self.Name)
		}
		return v, nil
	} else if self.Type == "float64" {
		bits := binary.LittleEndian.Uint64(b)
		v := math.Float64frombits(bits)
		return v, nil
	} else if self.Type == "string" {
		n := bytes.IndexByte(b, 0)
		v := string(b[:n])
		return v, nil
	} else if self.Type == "time" {
		var v time.Time
		err := v.UnmarshalBinary(b)
		if err != nil {
			return nil, err
		}
		return v, nil
	} else {
		return nil, errors.New("Type is not valid: " + self.Name)
	}
}

//**************************************************

func (self *Table) openConfigFile(configfilename string) error {

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
func (self *Table) openTableFile(tablefilename string) error {
	f, err := os.OpenFile(tablefilename, os.O_RDWR+os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	self.tablefile = f

	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.tablefile.ReadAt(b, 0)
	if err != nil {
		if err == io.EOF {
			self.fileVersion = 1
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
		if self.fileVersion != 1 {
			return errors.New("Fileversion is not correct")
		}
	}

	return err
}

func (self *Table) saveConfigFile(configfile string) error {
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

func (self *Table) setColumns(columnTypes []ColumnType) error {
	self.columnTypes = columnTypes
	self.columnBytes = 0
	for _, val := range columnTypes {
		num, err := val.GetBytes()
		if err != nil {
			return err
		}
		val.Size = num
		self.columnBytes = self.columnBytes + num
	}
	return nil
}

func (self *Table) convertRowNumToOffset(rowNum int64) int64 {
	offset := int64(rowNum)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64)
	return offset
}
func (self *Table) convertOffsetToRowNum(offset int64) int64 {
	rowNum := int64((offset - int64(binary.MaxVarintLen64)) / (int64(self.columnBytes + 1)))
	return rowNum
}

func (self *Table) searchLastOff() (int64, error) {
	lastOff, err := self.tablefile.Seek(0, 2)
	if err != nil {
		return -1, err
	}
	rowNum := self.convertOffsetToRowNum(lastOff)
	return rowNum, err
}
