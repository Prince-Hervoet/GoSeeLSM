package core

import (
	"os"
	"path"
	"someIndex/src/log"
	"someIndex/src/util"
	"strconv"
	"sync"
	"time"
)

const (
	WRITE = 0
	DELETE = 1
	WalFileCoreName = "letWal"
	WalImmuFileCoreName = "immu_LetWal"
	WalFileExName = ".log"
	ImmuWalFileExName = ".immu"
)

// Wal WAL记录器，用于写入操作的日志
type Wal struct {
	dir  	string
	Path 	string
	mm   	*sync.Mutex
}

type KeyValueBlock struct {
	key string
	value []byte
	isDel bool
}

// NewWal 获取一个wal对象，并初始化文件
func NewWal(dir string) *Wal {
	if dir == "" {
		log.EasyInfoLog("目录不能为空")
		return nil
	}
	wal := &Wal{}
	wPath := path.Join(dir, WalFileCoreName+WalFileExName)
	wal.Path = wPath
	wal.dir = dir
	file, b := util.MyOpenFile_RCA(wPath)
	if !b {
		return nil
	}
	defer util.MyCloseFile(file)
	wal.mm = &sync.Mutex{}
	return wal
}

// WriteLog 写入wal日志
func (wal *Wal) WriteLog(key string,value []byte) bool {
	if key == "" {
		log.EasyInfoLog("键值不能为空")
		return false
	}
	file,b := util.MyOpenFile_RCA(wal.Path)
	if !b {
		log.EasyInfoLog("打开wal日志文件失败")
		return false
	}
	defer util.MyCloseFile(file)
	keyBytes := []byte(key)
	kl := len(keyBytes)
	//写入操作类型
	util.Int8FileWrite(file,int8(WRITE))
	//写入key的长度
	util.Int64FileWrite(file,int64(kl))
	//写入key
	util.BytesFileWrite(file,keyBytes)
	//写入value的长度
	util.Int64FileWrite(file,int64(len(value)))
	//写入value
	util.BytesFileWrite(file,value)
	return true
}

// DeleteLog 写入删除日志
func (wal *Wal) DeleteLog(key string) bool {
	if key == "" {
		log.EasyInfoLog("键值不能为空")
		return false
	}
	file, b := util.MyOpenFile_RCA(wal.Path)
	if !b {
		log.EasyInfoLog("打开文件失败")
		return false
	}
	defer util.MyCloseFile(file)
	keyBytes := []byte(key)
	//写入类型
	util.Int8FileWrite(file,int8(DELETE))
	//写入key的长度
	util.Int64FileWrite(file,int64(len(keyBytes)))
	//写入key
	util.BytesFileWrite(file,keyBytes)
	return true
}

// ConvertImmuLog 将日志更改为不可变
func (wal *Wal) ConvertImmuLog() string {
	fileName := WalImmuFileCoreName + strconv.FormatInt(time.Now().UnixNano(),15) + ImmuWalFileExName
	//将原来的日志文件更名
	os.Rename(wal.Path,path.Join(wal.dir,fileName))
	file, b := util.MyOpenFile_RCA(wal.Path)
	if !b {
		return ""
	}
	defer util.MyCloseFile(file)
	return fileName
}

// RemoveImmuLogFile 移除不可变日志
func (wal *Wal) RemoveImmuLogFile(fileName string) {
	err := os.Remove(path.Join(wal.dir, fileName))
	if err != nil {
		log.EasyInfoLog("删除文件错误")
		return
	}
}