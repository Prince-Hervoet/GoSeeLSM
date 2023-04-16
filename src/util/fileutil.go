package util

import (
	"bytes"
	"encoding/binary"
	"os"
	"someIndex/src/log"
)

// MyOpenFile_RCA 打开文件
func MyOpenFile_RCA(path string) (*os.File,bool) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return nil,false
	}
	return file,true
}


func MyOpenFile_RONLY(path string) (*os.File,bool) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return nil,false
	}
	return file,true
}


func MyOpenAndCloseFile(path string) (*os.File,bool) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return nil,false
	}

	return file,true
}



func MyCloseFile(file *os.File)  {
	if file != nil {
		err := file.Close()
		if err != nil {
			log.EasyInfoLog(err.Error())
		}
	}
}

// BytesFileWrite 将字节数组写入指定文件
func BytesFileWrite(file *os.File,value []byte) bool {
	write, err := file.Write(value)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return false
	}
	if write != 0 {
		return true
	}
	return false
}

// Int64FileWrite 将64位整型数据写入文件
func Int64FileWrite(file *os.File,num int64) bool {
	bs := bytes.NewBuffer([]byte{})
	err := binary.Write(bs, binary.BigEndian, &num)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return false
	}
	_, err2 := file.Write(bs.Bytes())
	if err2 != nil {
		log.EasyInfoLog(err2.Error())
		return false
	}
	return true
}

// Int8FileWrite 将8位整型数据写入文件
func Int8FileWrite(file *os.File,num int8) bool {
	bs := bytes.NewBuffer([]byte{})
	err := binary.Write(bs, binary.BigEndian, &num)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return false
	}
	_, err2 := file.Write(bs.Bytes())
	if err2 != nil {
		log.EasyInfoLog(err2.Error())
		return false
	}
	return true
}

// Int64FileRead 从文件中读取一个64位整型
func Int64FileRead(file *os.File) (int64,bool) {
	buff := make([]byte,8)
	_, err := file.Read(buff)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return 0,false
	}
	bs := bytes.NewBuffer(buff)
	var ans int64
	err = binary.Read(bs, binary.BigEndian, &ans)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return 0,false
	}
	return ans,true
}


// Int8FileRead 从文件中读取一个8位整型
func Int8FileRead(file *os.File) (int8,bool) {
	buff := make([]byte,1)
	r, err := file.Read(buff)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return 0,false
	}
	if r == 0 {
		return 0,false
	}
	bs := bytes.NewBuffer(buff)
	var ans int8
	err = binary.Read(bs, binary.BigEndian, &ans)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return 0,false
	}
	return ans,true
}

// BytesFileRead 从文件中读取字节数组
func BytesFileRead(file *os.File,len int64) ([]byte,bool) {
	buff := make([]byte,len)
	_, err := file.Read(buff)
	if err != nil {
		log.EasyInfoLog(err.Error())
		return nil,false
	}
	return buff,true
}
