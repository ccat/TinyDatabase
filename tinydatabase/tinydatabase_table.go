/*
 Package tinydatabase provides simple database functions.
*/
package tinydatabase

import (
	"bytes"
	"encoding/binary"
	//"encoding/json"
	"errors"
	//"fmt"
	//"io"
	//"io/ioutil"
	"math"
	//"os"
	//"path"
	//"strconv"
	"time"
)

type ColumnType struct {
	Name string
	Type string
	Size int64 //When Size is 0, size of the column can be variable
}

type Row map[string]interface{}

type TableInterface interface {
	NewTable(directory string, tablename string, columnTypes []ColumnType) error
	Open(directory string, tablename string) error
	Close() error
	ReadRow(rowNum int64) (Row, error)
	WriteRow(row Row) (int64, error)
	DeleteRow(rowNum int64) error
	GetTableType() string
}

const (
	UNKNOWN        int64 = 0
	STATIC1        int64 = 1
	DYNAMIC1_TABLE int64 = 2
	DYNAMIC1_INDEX int64 = 3
)

const (
	ROW_DELETED byte = 0
	ROW_NORMAL  byte = 1
)

const (
	COLUMN_INT64   string = "int64"
	COLUMN_FLOAT64 string = "float64"
	COLUMN_STRING  string = "string"
	COLUMN_TIME    string = "time"
)

func (self *ColumnType) GetBytes() (int64, error) {
	if self.Type == "int64" {
		return binary.MaxVarintLen64, nil
	} else if self.Type == "float64" {
		return 8, nil
	} else if self.Type == "string" {
		if self.Size < 0 {
			return 0, errors.New("Size is not valid")
		}
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
	if byteNum == 0 {
		byteNum = 1
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
		var v int64
		switch vi := val.(type) {
		case int64:
			v = vi //.(int64)
		case int:
			v = int64(vi)
		case int8:
			v = int64(vi)
		case int16:
			v = int64(vi)
		case int32:
			v = int64(vi)
		default:
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
		if byteNum == 0 {
			byteNum = int64(len(v))
		}
		b = make([]byte, byteNum)
		if int64(len(v)) > byteNum {
			return nil, errors.New("Too long string for " + self.Name)
		}
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
		if n == -1 {
			n = len(b)
		}
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
