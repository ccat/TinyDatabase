package tinydatabase

import (
	//"fmt"
	"os"
	"encoding/binary"
	//"io/ioutil"
	//"encoding/json"
	"errors"
	"io"
	"math"
	"bytes"
	"time"
)


type ColumnType struct {
	Name string
	Type string
}

type Table struct {
	Filename string
	file *os.File
	fileVersion int64
	columnTypes []ColumnType
	columnBytes uint64
}

func (self *ColumnType) GetBytes() (uint64,error){
	if(self.Type=="int64"){
		return binary.MaxVarintLen64,nil
	}else if(self.Type=="float64"){
		return 8,nil
	}else if(self.Type=="string256"){
		return 256,nil
	}else if(self.Type=="time"){
		return 15,nil
	}
	return 0,errors.New("Type is not valid")
}

func (self *ColumnType) GetNil() ([]byte,error){
	var b []byte
	byteNum,err:=self.GetBytes()
	if(err!=nil){
		return nil,err
	}
	b=make([]byte,byteNum)
	if(self.Type=="int64"){
		binary.PutVarint(b, int64(0))
		return b,nil
	}else if(self.Type=="float64"){
    	bits := math.Float64bits(0.0)
	    binary.LittleEndian.PutUint64(b, bits)
		return b,nil
	}else if(self.Type=="string256"){
		return b,nil
	}else if(self.Type=="time"){
		b,err = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC).MarshalBinary()
		if(err!=nil){
			return nil,err
		}
		return b,nil
	}else{
		return nil,errors.New("Type is not valid")
	}
}

func (self *ColumnType) ConvertBytes(val interface{}) ([]byte,error){
	var b []byte
	byteNum,err:=self.GetBytes()
	if(err!=nil){
		return nil,err
	}
	if(self.Type=="int64"){
		v,ok:=val.(int64)
		if(ok==false){
			return nil,errors.New("Missmatch type(int64) and val: "+self.Name)
		}
		b=make([]byte,byteNum)
		binary.PutVarint(b, v)
		return b,nil
	}else if(self.Type=="float64"){
		v,ok:=val.(float64)
		if(ok==false){
			return nil,errors.New("Missmatch type(float64) and val: "+self.Name)
		}
		b=make([]byte,byteNum)
    	bits := math.Float64bits(v)
	    binary.LittleEndian.PutUint64(b, bits)
		return b,nil
	}else if(self.Type=="string256"){
		v,ok:=val.(string)
		if(ok==false){
			return nil,errors.New("Missmatch type(string256) and val: "+self.Name)
		}
		b=make([]byte,byteNum)
		for i := 0; i < len(v); i++ {
			b[i]=v[i]
		}
		return b,nil
	}else if(self.Type=="time"){
		v,ok:=val.(time.Time)
		if(ok==false){
			return nil,errors.New("Missmatch type(time) and val: "+self.Name)
		}
		b,err=v.MarshalBinary()
		if(err!=nil){
			return nil,err
		}
		return b,nil
	}else{
		return nil,errors.New("Type is not valid: "+self.Name)
	}
}

func (self *ColumnType) ConvertVal(b []byte) (interface{},error){
	if(self.Type=="int64"){
		var v int64
		v,num:=binary.Varint(b)
		if(num<1){
			return nil,errors.New("Missmatch type(int64) and val: "+self.Name)
		}
		return v,nil
	}else if(self.Type=="float64"){
    	bits := binary.LittleEndian.Uint64(b)
	    v := math.Float64frombits(bits)
		return v,nil
	}else if(self.Type=="string256"){
		n := bytes.IndexByte(b, 0)
		v := string(b[:n])
		return v,nil
	}else if(self.Type=="time"){
		var v time.Time
		err := v.UnmarshalBinary(b)
		if(err!=nil){
			return nil,err
		}
		return v,nil
	}else{
		return nil,errors.New("Type is not valid: "+self.Name)
	}
}

func NewTable(filename string, columnTypes []ColumnType) (*Table, error){
	var tableInst *Table = new(Table)
	err:=tableInst.OpenFile(filename)
	if(err!=nil){
		return nil,err
	}
	err=tableInst.SetColumns(columnTypes)
	if(err!=nil){
		return nil,err
	}
	return tableInst,nil
}

func (self *Table) OpenFile(filename string) (error){
	self.Filename=filename
	f,err := os.OpenFile(filename,os.O_RDWR+os.O_CREATE,0666)
	if(err!=nil){
		return err
	}
	self.file=f
	
	b := make([]byte, binary.MaxVarintLen64)
	num,err:=self.file.ReadAt(b,0)
	if(err!=nil){
		if(err==io.EOF){
			self.fileVersion=1
			binary.PutVarint(b, self.fileVersion)
			num,err = self.file.WriteAt(b,0)
			if(err!=nil){
				return err
			}
		}else{
			return err
		}
	}else{
		self.fileVersion,num = binary.Varint(b)
		if(num==0){
			return errors.New("Failed to read fileversion")
		}
	}
	
	return err
}

func (self *Table) Close() (error){
	self.Filename=""
	self.file.Close()
	self.file=nil
	return nil
}

func (self *Table) SetColumns(columnTypes []ColumnType) (error){
	self.columnTypes=columnTypes
	self.columnBytes=0
	for _,val := range columnTypes {
		num,err := val.GetBytes()
		if(err!=nil){
			return err
		}
		self.columnBytes=self.columnBytes+num
	}
	return nil
}

func (self *Table) WriteRow(rowNum int64,row map[string]interface{}) (error){
	targetOff := rowNum
	var b []byte
	b=make([]byte,1)
	b[0]=1
	num,err := self.file.WriteAt(b,targetOff)
	if(err!=nil){
		return err
	}
	targetOff=targetOff+int64(num)
	for _,v := range self.columnTypes{
		if val, ok := row[v.Name]; ok {
			b,err=v.ConvertBytes(val)
			if(err!=nil){
				return err
			}
		}else{
			b,err=v.GetNil()
			if(err!=nil){
				return err
			}
		}
		num,err := self.file.WriteAt(b,targetOff)
		if(err!=nil){
			return err
		}
		targetOff=targetOff+int64(num)
	}
	err=self.file.Sync()
	if(err!=nil){
		return err
	}
	return nil
}

func (self *Table) Update(rowNum int,row map[string]interface{}) (error){
	targetOff := int64(rowNum)*int64(self.columnBytes+1)+int64(binary.MaxVarintLen64)
	return self.WriteRow(targetOff,row)
}
	
func (self *Table) Insert(row map[string]interface{}) (int,error){
	lastOff,err := self.file.Seek(0,2)
	if(err!=nil){
		return -1,err
	}
	var rowNum int 
	rowNum= int((lastOff-int64(binary.MaxVarintLen64))/(int64(self.columnBytes+1)))
	err = self.WriteRow(lastOff,row)
	return rowNum,err
}

func (self *Table) Read(num int) (map[string]interface{},error){
	targetOff := int64(num)*int64(self.columnBytes+1)+int64(binary.MaxVarintLen64)

	var b []byte
	b=make([]byte,1)
	num,err := self.file.ReadAt(b,targetOff)
	if(err!=nil){
		return nil,err
	}
	if(b[0]==0){
		return nil,errors.New("Deleted row")
	}
	targetOff=targetOff+1

	var result map[string]interface{}
	result = make(map[string]interface{})
	for _,v := range self.columnTypes{
		b,err=v.GetNil()
		if(err!=nil){
			return nil,err
		}
		_,err = self.file.ReadAt(b,targetOff)
		if(err!=nil){
			return nil,err
		}
		result[v.Name],err=v.ConvertVal(b)
		if(err!=nil){
			return nil,err
		}
		nextOff,err:=v.GetBytes()
		if(err!=nil){
			return nil,err
		}
		targetOff=targetOff+int64(nextOff)
	}
	return result,nil
}

func (self *Table) Delete(rowNum int) (error){
	targetOff := int64(rowNum)*int64(self.columnBytes+1)+int64(binary.MaxVarintLen64)
	var b []byte
	b=make([]byte,1)
	b[0]=0
	_,err := self.file.WriteAt(b,targetOff)
	err=self.file.Sync()
	if(err!=nil){
		return err
	}
	return nil
}

