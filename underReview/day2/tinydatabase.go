package tinydatabase

import (
	//"fmt"
	"encoding/binary"
	"os"
	//"io/ioutil"
	//"encoding/json"
	"bytes"
	"errors"
	"io"
	"math"
	"time"
)


type ColumnType int

const (
	CT_Int64 ColumnType = iota+1
	CT_Float64 
	CT_String256 
	CT_Time
)

type ColumnConfig struct {
	Name string
	Type ColumnType
}

type Table struct {
	Filename    string
	file        *os.File
	fileVersion int64
	ColumnConfigs []ColumnConfig
	columnBytes uint64
}

type Row map[string]interface{}

type Condition struct {
	TargetColumn ColumnConfig
	LookupCondition int
	Value interface{}
}

const (
	CONDITION_Equal  = iota
	CONDITION_LessThan 
	CONDITION_GreaterThan 
	CONDITION_LessThanOrEqual
	CONDITION_GreaterThanOrEqual
)

func (self ColumnType) String() string {
    switch self {
    case CT_Int64:
        return "int64"
    case CT_Float64:
        return "float64"
    case CT_String256:
        return "string256"
    case CT_Time:
        return "time"
    default:
        return "Unknown"
    }
}

func (self *Condition) Check(row Row) (bool) {
	if val, ok := row[self.TargetColumn.Name]; ok {
		switch val.(type) {
		case int64:
			return self.checkInt64(val.(int64))
		case float64:
			return self.checkFloat64(val.(float64))
		case string:
			return self.checkString(val.(string))
		case time.Time:
			return self.checkTime(val.(time.Time))
		}
    }
	return false
}

func (self *Condition) checkInt64(val int64) (bool) {
	myVal := self.Value.(int64)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val==myVal {return true}
	case CONDITION_LessThan:
		if val<myVal {return true}
	case CONDITION_GreaterThan:
		if val>myVal {return true}
	case CONDITION_LessThanOrEqual:
		if val<=myVal {return true}
	case CONDITION_GreaterThanOrEqual:
		if val>=myVal {return true}
	}
	return false	
}

func (self *Condition) checkFloat64(val float64) (bool) {
	myVal := self.Value.(float64)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val==myVal {return true}
	case CONDITION_LessThan:
		if val<myVal {return true}
	case CONDITION_GreaterThan:
		if val>myVal {return true}
	case CONDITION_LessThanOrEqual:
		if val<=myVal {return true}
	case CONDITION_GreaterThanOrEqual:
		if val>=myVal {return true}
	}
	return false	
}

func (self *Condition) checkTime(val time.Time) (bool) {
	diff := self.Value.(time.Time).Sub(val)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if diff==0 {return true}
	case CONDITION_LessThan:
		if diff>0 {return true}
	case CONDITION_GreaterThan:
		if diff<0 {return true}
	case CONDITION_LessThanOrEqual:
		if diff>=0 {return true}
	case CONDITION_GreaterThanOrEqual:
		if diff<=0 {return true}
	}
	return false	
}

func (self *Condition) checkString(val string) (bool) {
	myVal := self.Value.(string)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val==myVal {return true}
	}
	return false	
}

func (self *ColumnConfig) GetBytes() (uint64, error) {
    switch self.Type {
    case CT_Int64:
		return binary.MaxVarintLen64, nil
    case CT_Float64:
		return 8, nil
    case CT_String256:
		return 256, nil
    case CT_Time:
		return 15, nil
    default:
		return 0, errors.New("Type is not valid")
    }
}

func (self *ColumnConfig) GetNil() ([]byte, error) {
	var b []byte
	byteNum, err := self.GetBytes()
	if err != nil {
		return nil, err
	}
	b = make([]byte, byteNum)

    switch self.Type {
    case CT_Int64:
		binary.PutVarint(b, int64(0))
		return b, nil
    case CT_Float64:
		bits := math.Float64bits(0.0)
		binary.LittleEndian.PutUint64(b, bits)
		return b, nil
    case CT_String256:
		return b, nil
    case CT_Time:
		b, err = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC).MarshalBinary()
		if err != nil {
			return nil, err
		}
		return b, nil
    default:
		return nil, errors.New("Type is not valid")
    }
}

func (self *ColumnConfig) ConvertBytes(val interface{}) ([]byte, error) {
	var b []byte
	byteNum, err := self.GetBytes()
	if err != nil {
		return nil, err
	}

    switch self.Type {
    case CT_Int64:
		v, ok := val.(int64)
		if ok == false {
			return nil, errors.New("Missmatch type(int64) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		binary.PutVarint(b, v)
		return b, nil
    case CT_Float64:
		v, ok := val.(float64)
		if ok == false {
			return nil, errors.New("Missmatch type(float64) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		bits := math.Float64bits(v)
		binary.LittleEndian.PutUint64(b, bits)
		return b, nil
    case CT_String256:
		v, ok := val.(string)
		if ok == false {
			return nil, errors.New("Missmatch type(string256) and val: " + self.Name)
		}
		b = make([]byte, byteNum)
		for i := 0; i < len(v); i++ {
			b[i] = v[i]
		}
		return b, nil
    case CT_Time:
		v, ok := val.(time.Time)
		if ok == false {
			return nil, errors.New("Missmatch type(time) and val: " + self.Name)
		}
		b, err = v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return b, nil
    default:
		return nil, errors.New("Type is not valid: " + self.Name)
    }
}

func (self *ColumnConfig) ConvertVal(b []byte) (interface{}, error) {
    switch self.Type {
    case CT_Int64:
		var v int64
		v, num := binary.Varint(b)
		if num < 1 {
			return nil, errors.New("Missmatch type(int64) and val: " + self.Name)
		}
		return v, nil
    case CT_Float64:
		bits := binary.LittleEndian.Uint64(b)
		v := math.Float64frombits(bits)
		return v, nil
    case CT_String256:
		n := bytes.IndexByte(b, 0)
		v := string(b[:n])
		return v, nil
    case CT_Time:
		var v time.Time
		err := v.UnmarshalBinary(b)
		if err != nil {
			return nil, err
		}
		return v, nil
    default:
		return nil, errors.New("Type is not valid: " + self.Name)
    }
}

func NewTable(filename string, columnConfigs []ColumnConfig) (*Table, error) {
	var tableInst *Table = new(Table)
	err := tableInst.OpenFile(filename)
	if err != nil {
		return nil, err
	}
	err = tableInst.setColumns(columnConfigs)
	if err != nil {
		return nil, err
	}
	return tableInst, nil
}

func (self *Table) OpenFile(filename string) error {
	self.Filename = filename
	f, err := os.OpenFile(filename, os.O_RDWR+os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	self.file = f

	b := make([]byte, binary.MaxVarintLen64)
	num, err := self.file.ReadAt(b, 0)
	if err != nil {
		if err == io.EOF {
			self.fileVersion = 1
			binary.PutVarint(b, self.fileVersion)
			num, err = self.file.WriteAt(b, 0)
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
	}

	return err
}

func (self *Table) Close() error {
	if self.file == nil {
		return errors.New("Already closed")
	}
	self.Filename = ""
	self.file.Close()
	self.file = nil
	return nil
}

func (self *Table) setColumns(columnConfigs []ColumnConfig) error {
	self.ColumnConfigs = columnConfigs
	self.columnBytes = 0
	for _, val := range columnConfigs {
		num, err := val.GetBytes()
		if err != nil {
			return err
		}
		self.columnBytes = self.columnBytes + num
	}
	return nil
}

func (self *Table) WriteRow(rowNum int64, row Row) error {
	var b []byte
	var insertData map[string][]byte
	insertData = make(map[string][]byte)
	//Data type check
	for _, v := range self.ColumnConfigs {
		if val, ok := row[v.Name]; ok {
			b, err := v.ConvertBytes(val)
			if err != nil {
				return err
			}
			insertData[v.Name] = b
		} else {
			b, err := v.GetNil()
			if err != nil {
				return err
			}
			insertData[v.Name] = b
		}
	}

	targetOff := rowNum
	b = make([]byte, 1)
	b[0] = 1
	num, err := self.file.WriteAt(b, targetOff)
	if err != nil {
		return err
	}
	targetOff = targetOff + int64(num)
	for _, v := range self.ColumnConfigs {
		num, err := self.file.WriteAt(insertData[v.Name], targetOff)
		if err != nil {
			return err
		}
		targetOff = targetOff + int64(num)
	}
	err = self.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (self *Table) Update(rowNum int, row Row) error {
	targetOff := int64(rowNum)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64)
	return self.WriteRow(targetOff, row)
}

func (self *Table) Insert(row Row) (int, error) {
	lastNum, err := self.GetLastNum()
	lastOff := int64(lastNum)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64) //To ignore trash data

	err = self.WriteRow(lastOff, row)
	return lastNum, err
}

func (self *Table) GetLastNum() (int, error) { // To return next data write point
	lastOff, err := self.file.Seek(0, 2)
	if err != nil {
		return -1, err
	}
	var rowNum int
	rowNum = int((lastOff - int64(binary.MaxVarintLen64)) / (int64(self.columnBytes + 1)))
	return rowNum, err
}

func (self *Table) Select(condition []Condition) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	lastNum, err := self.GetLastNum()
	if err != nil {
		return nil, err
	}
	for i := 0; i < lastNum; i++ {
		testRow, err := self.Read(i)
		if err != nil {
			if err.Error() == "Deleted row" {
				continue
			}
			return nil, err
		}

		flag := true
		for _, v := range condition {
			if v.Check(testRow)==false {
				flag = false
				break
			}
		}
		if flag == true {
			result = append(result, testRow)
		}
	}
	return result, nil
}

func (self *Table) Read(num int) (Row, error) {
	targetOff := int64(num)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64)

	var b []byte
	b = make([]byte, 1)
	num, err := self.file.ReadAt(b, targetOff)
	if err != nil {
		return nil, err
	}
	if b[0] == 0 {
		return nil, errors.New("Deleted row")
	}
	targetOff = targetOff + 1

	var result Row
	result = make(Row)
	for _, v := range self.ColumnConfigs {
		b, err = v.GetNil()
		if err != nil {
			return nil, err
		}
		_, err = self.file.ReadAt(b, targetOff)
		if err != nil {
			return nil, err
		}
		result[v.Name], err = v.ConvertVal(b)
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

func (self *Table) Delete(rowNum int) error {
	targetOff := int64(rowNum)*int64(self.columnBytes+1) + int64(binary.MaxVarintLen64)
	var b []byte
	b = make([]byte, 1)
	b[0] = 0
	_, err := self.file.WriteAt(b, targetOff)
	err = self.file.Sync()
	if err != nil {
		return err
	}
	return nil
}
