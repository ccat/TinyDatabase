package tinydatabase

import (
	"encoding/json"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"time"
	//"fmt"
)

type ColumnType int

const (
	CT_Int64 ColumnType = iota + 1
	CT_Float64
	CT_String256
	CT_Time
)

type ColumnConfig struct {
	Name string
	Type ColumnType
}

type Table struct {
	Filename      string
	file          *os.File
	fileVersion   int64
	ColumnConfigs []ColumnConfig
	columnBytes   uint64
}

type Row map[string]interface{}

type Database struct {
	ConfigFilename string
	DataDir        string
	fileVersion    int64
	Tables         map[string]*Table
}

type Condition struct {
	TargetColumn    ColumnConfig
	LookupCondition int
	Value           interface{}
}

const (
	CONDITION_Equal = iota
	CONDITION_LessThan
	CONDITION_GreaterThan
	CONDITION_LessThanOrEqual
	CONDITION_GreaterThanOrEqual
)

type CommandType int

const (
	C_Select CommandType = iota + 1
	C_Update
	C_Insert
	C_Drop
)

type Sentence struct {
	Command CommandType
	TableName string
	Where []Condition
	RowData Row
}

type JsonColumnConfig struct {
	Name string
	Type string
}

type JsonCondition struct {
	TargetColumn    JsonColumnConfig
	LookupCondition string
	Value           interface{}
}

type JsonSentence struct {
	Command string
	TableName string
	Where []JsonCondition
	RowData Row
}

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

func (self CommandType) String() string {
	switch self {
	case C_Select:
		return "Select"
	case C_Update:
		return "Update"
	case C_Insert:
		return "Insert"
	case C_Drop:
		return "Drop"
	default:
		return "Unknown"
	}
}

func (self *Condition) Check(row Row) bool {
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

func (self *Condition) checkInt64(val int64) bool {
	myVal := self.Value.(int64)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val == myVal {
			return true
		}
	case CONDITION_LessThan:
		if val < myVal {
			return true
		}
	case CONDITION_GreaterThan:
		if val > myVal {
			return true
		}
	case CONDITION_LessThanOrEqual:
		if val <= myVal {
			return true
		}
	case CONDITION_GreaterThanOrEqual:
		if val >= myVal {
			return true
		}
	}
	return false
}

func (self *Condition) checkFloat64(val float64) bool {
	myVal := self.Value.(float64)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val == myVal {
			return true
		}
	case CONDITION_LessThan:
		if val < myVal {
			return true
		}
	case CONDITION_GreaterThan:
		if val > myVal {
			return true
		}
	case CONDITION_LessThanOrEqual:
		if val <= myVal {
			return true
		}
	case CONDITION_GreaterThanOrEqual:
		if val >= myVal {
			return true
		}
	}
	return false
}

func (self *Condition) checkTime(val time.Time) bool {
	diff := self.Value.(time.Time).Sub(val)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if diff == 0 {
			return true
		}
	case CONDITION_LessThan:
		if diff > 0 {
			return true
		}
	case CONDITION_GreaterThan:
		if diff < 0 {
			return true
		}
	case CONDITION_LessThanOrEqual:
		if diff >= 0 {
			return true
		}
	case CONDITION_GreaterThanOrEqual:
		if diff <= 0 {
			return true
		}
	}
	return false
}

func (self *Condition) checkString(val string) bool {
	myVal := self.Value.(string)
	switch self.LookupCondition {
	case CONDITION_Equal:
		if val == myVal {
			return true
		}
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
	if err != nil {
		return -1, err
	}
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

func (self *Table) Select(condition []Condition) (map[int]Row, error) {
	var result map[int]Row
	result = make(map[int]Row)
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
			if v.Check(testRow) == false {
				flag = false
				break
			}
		}
		if flag == true {
			result[i]=testRow
			//result = append(result, testRow)
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

func NewDatabase(filename string) (*Database, error) {
	if filename == "" {
		return nil, errors.New("No file name passed")
	}

	var dbInst *Database = new(Database)
	_, err := os.Stat(filename)
	if err != nil {
		dbInst.fileVersion = 1
		dbInst.ConfigFilename = filename
		dbInst.DataDir = "./data/"
		dbInst.Tables =	make(map[string]*Table)
		if err := os.MkdirAll(dbInst.DataDir, 0755); err != nil {
			return nil, err
		}
		err = dbInst.Save()
		return dbInst, err
	}

	err = dbInst.LoadConfig(filename)
	if err != nil {
		return nil, err
	}
	return dbInst, nil
}

func (self *Database) LoadConfig(filename string) error {

	jsonString, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonString, &self)
	if err != nil {
		return err
	}
	for _, table := range self.Tables {
		err = table.OpenFile(table.Filename)
		if err != nil {
			return err
		}
		err = table.setColumns(table.ColumnConfigs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Database) Save() error {
	b, err := json.Marshal(self)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(self.ConfigFilename, b, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (self *Database) Close() error {
	err := self.Save()
	if err != nil{
		return err
	}
	for _, table := range self.Tables {
		err:=table.Close()
		if err != nil {
			return err
		}
	}
	self.ConfigFilename=""
	return nil
}

func (self *Database) CreateTable(tableName string, columnConfigs []ColumnConfig) (error) {
	if _, exist := self.Tables[tableName]; exist {
		return errors.New("Table already exists")
    }
	tableInst,err := NewTable(self.DataDir+tableName, columnConfigs)
	if err != nil{
		return err
	}
	self.Tables[tableName]=tableInst
	return nil
}

func (self *Database) Exec (sentence Sentence) (map[int]Row,error) {
	result := make(map[int]Row)
	
	targetTable,exist := self.Tables[sentence.TableName]
	if exist!= true {
		return nil,errors.New("Table does not exist")
	}
	switch sentence.Command {
	case C_Select:
		return targetTable.Select(sentence.Where)
	case C_Update:
		targets,err := targetTable.Select(sentence.Where)
		if err != nil {
			return nil,err
		}
		for rowNum,_ := range targets {
			err=targetTable.Update(rowNum,sentence.RowData)
			if err != nil {
				return nil,err
			}
			result[rowNum]=sentence.RowData
		}
	case C_Insert:
		rowNum,err :=targetTable.Insert(sentence.RowData)
		if err != nil {
			return nil,err
		}
		/*row,err := targetTable.Read(rowNum)
		if err != nil {
			return nil,err
		}*/
		result[rowNum]=sentence.RowData
	case C_Drop:
		targets,err := targetTable.Select(sentence.Where)
		if err != nil {
			return nil,err
		}
		for rowNum,val := range targets {
			err=targetTable.Delete(rowNum)
			if err != nil {
				return nil,err
			}
			result[rowNum]=val
		}
	default:
		return nil,errors.New("Invalid Command")
	}
	return result,nil
}

func (self *Database) convertJson (sentence JsonSentence) (Sentence,error) {
	var result Sentence
	
	switch sentence.Command {
	case "Select":
		result.Command=C_Select
	case "Update":
		result.Command=C_Update
	case "Insert":
		result.Command=C_Insert
	case "Drop":
		result.Command=C_Drop
	default:
		return result,errors.New("Invalid Command")
	}

	result.TableName=sentence.TableName
	result.RowData=sentence.RowData

	for _,val := range sentence.Where {
		var tempCond Condition
		tempCond.Value=val.Value
		switch val.LookupCondition {
		case "Equal":
			tempCond.LookupCondition=CONDITION_Equal
		case "LessThan":
			tempCond.LookupCondition=CONDITION_LessThan
		case "GreaterThan":
			tempCond.LookupCondition=CONDITION_GreaterThan
		case "LessThanOrEqual":
			tempCond.LookupCondition=CONDITION_LessThanOrEqual
		case "GreaterThanOrEqual":
			tempCond.LookupCondition=CONDITION_GreaterThanOrEqual
		default:
			return result,errors.New("Invalid Condition")
		}
		tempCond.TargetColumn.Name=val.TargetColumn.Name
		switch val.TargetColumn.Type {
		case "Int64":
			tempCond.TargetColumn.Type=CT_Int64
		case "Float64":
			tempCond.TargetColumn.Type=CT_Float64
		case "String256":
			tempCond.TargetColumn.Type=CT_String256
		case "Time":
			tempCond.TargetColumn.Type=CT_Time
		default:
			return result,errors.New("Invalid ColumnType")
		}
		result.Where=append(result.Where,tempCond)
	}
	return result,nil
}

func (self *Database) ExecJson (data []byte) (map[int]Row,error) {
	var sentence JsonSentence
	err = json.Unmarshal(data, &sentence)
	if err != nil{
		return nil,err
	}
	sent,err := self.convertJson(sentence)
	if err != nil{
		return nil,err
	}
	return self.Exec(sent)
}
