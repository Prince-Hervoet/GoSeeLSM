package core

import (
	"io/ioutil"
	"path"
	"someIndex/src/data_structure"
	"someIndex/src/log"
	"someIndex/src/util"
	"strconv"
	"sync"
	"time"
)

const (
	// ListLenThreshold 原生数据链长度阈值，大于这个值就需要进行转换
	ListLenThreshold = 2000
)

var immuChannel = make(chan *immuMemUnit,10)

// MemTable 内存表数据结构（跳表），用于缓存插入的数据
type MemTable struct {
	//当前可用的内存表结构
	ActiveMemList *data_structure.SkipList
	//不可变内存表缓存
	immuMemList *data_structure.SkipList
	//WAL日志记录器
	wal *Wal
	//🔒
	mm sync.Mutex
}

//不可变内存表的结构
type immuMemUnit struct {
	//不可变的跳表
	sl *data_structure.SkipList
	//不可变的wal日志名
	walFileName string
}


// GetWal 获取wal记录器
func (mt *MemTable) GetWal() *Wal {
	return mt.wal
}

// NewMemTable 获取一个内存表
func NewMemTable(dir string) *MemTable {
	table := data_structure.NewSkipList()
	wal := NewWal(dir)
	return &MemTable{ActiveMemList: table,wal: wal,mm: sync.Mutex{}}
}

// Add 添加
func (mt *MemTable) Add(key string,value []byte) {
	startT := time.Now()
	//双重检测当前链表是否达到阈值
	if mt.ActiveMemList.NodeCount >= ListLenThreshold {
		mt.mm.Lock()
		if mt.ActiveMemList.NodeCount >= ListLenThreshold {
			//如果大于等于阈值则将当前的内存表转变成不可变内存表
			//将wal的日志更改
			fileName := mt.wal.ConvertImmuLog()
			immuUnit := &immuMemUnit{sl: mt.ActiveMemList,walFileName: fileName}
			mt.immuMemList = mt.ActiveMemList
			immuChannel <- immuUnit
			mt.ActiveMemList = data_structure.NewSkipList()
		}
		mt.mm.Unlock()
	}
	//先写入wal日志
	bo := mt.wal.WriteLog(key, value)
	if !bo {
		return
	}
	//如果没达到阈值则直接插入
	mt.mm.Lock()
	mt.ActiveMemList.Insert(key, value)
	mt.mm.Unlock()
	ct := time.Since(startT)
	log.EasyInfoLog("本次插入用时: " + strconv.FormatFloat(ct.Seconds(),'f', 10, 32) + " s")
}

// Search 搜索
func (mt *MemTable) Search(key string) []byte {
	startT := time.Now()
	//先从当前表搜索
	search,bo := mt.ActiveMemList.Search(key)
	if bo {
		ct := time.Since(startT)
		log.EasyInfoLog("本次查询用时: " + strconv.FormatFloat(ct.Seconds(),'f', 10, 32) + " s")
		return search.Value
	}

	//如果找不到就从不可变中搜索
	if mt.immuMemList != nil {
		search, bo := mt.immuMemList.Search(key)
		if bo {
			ct := time.Since(startT)
			log.EasyInfoLog("本次查询用时: " + strconv.FormatFloat(ct.Seconds(),'f', 10, 32) + " s")
			return search.Value
		}
	}

	//如果还是找不到就从磁盘中搜索
	treeSearch := gbsp.sst.SsTableTreeSearch(key)
	if treeSearch != nil {
		ct := time.Since(startT)
		log.EasyInfoLog("本次查询用时: " + strconv.FormatFloat(ct.Seconds(),'f', 10, 32) + " s")
		return treeSearch
	}
	ct := time.Since(startT)
	log.EasyInfoLog("本次查询用时: " + strconv.FormatFloat(ct.Seconds(),'f', 10, 32) + " s")
	return nil
}

// Delete 删除
func (mt *MemTable) Delete(key string) {
	bo := mt.wal.DeleteLog(key)
	if !bo {
		return
	}
	mt.ActiveMemList.Delete(key)
}

const (
	ActionTypeSize = 1
	LenSize = 8
)

// RecoverFromFile 从WAL文件恢复内存表
func (mt *MemTable)RecoverFromFile() {
	mt.mm.Lock()
	defer mt.mm.Unlock()

	wal := mt.wal
	//恢复active内存表
	recoverActiveMemList(wal.Path,mt)

	//恢复没Flush的不可变内存表
	dir, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		log.EasyInfoLog("恢复文件错误")
		return
	}
	for _,fi := range dir {
		if fi.IsDir() {
			continue
		}

		ext := path.Ext(fi.Name())
		if ext != ImmuWalFileExName {
			continue
		}

		fileName := fi.Name()
		RecoverImmuMemList(path.Join(wal.dir,fileName),fileName)
	}
}

func recoverActiveMemList(path string,mt *MemTable) bool {
	file, b := util.MyOpenFile_RONLY(path)
	if !b {
		return false
	}
	defer util.MyCloseFile(file)
	stat, err := file.Stat()
	if err != nil {
		return false
	}
	fileSize := stat.Size()

	run := int64(0)
	for run < fileSize {
		//读取操作类型
		read, b1 := util.Int8FileRead(file)
		if !b1 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += ActionTypeSize
		tNum := read
		//读取kl
		keyLen, b2 := util.Int64FileRead(file)
		if !b2 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += LenSize
		//读取key
		keyBytes,b3 := util.BytesFileRead(file, keyLen)
		if !b3 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += keyLen
		if tNum == 1 {
			mt.ActiveMemList.Delete(string(keyBytes))
		}
		//读取vl
		valueLen, b4 := util.Int64FileRead(file)
		if !b4 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += LenSize
		//读取value
		value, b5 := util.BytesFileRead(file, valueLen)
		if !b5 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += valueLen
		if tNum == 0 {
			mt.ActiveMemList.Insert(string(keyBytes),value)
		}
	}
	return true
}

func RecoverImmuMemList(path string,walFileName string) bool {
	file, b := util.MyOpenFile_RONLY(path)
	if !b {
		return false
	}
	defer util.MyCloseFile(file)
	stat, err := file.Stat()
	if err != nil {
		return false
	}
	fileSize := stat.Size()
	tempSkip := data_structure.NewSkipList()
	run := int64(0)
	for run < fileSize {
		//读取操作类型
		read, b1 := util.Int8FileRead(file)
		if !b1 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += ActionTypeSize
		tNum := read
		//读取kl
		keyLen, b2 := util.Int64FileRead(file)
		if !b2 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += LenSize
		//读取key
		keyBytes,b3 := util.BytesFileRead(file, keyLen)
		if !b3 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += keyLen
		if tNum == 1 {
			tempSkip.Delete(string(keyBytes))
		}
		//读取vl
		valueLen, b4 := util.Int64FileRead(file)
		if !b4 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += LenSize
		//读取value
		value, b5 := util.BytesFileRead(file, valueLen)
		if !b5 {
			log.EasyInfoLog("恢复有错误")
			return false
		}
		run += valueLen
		if tNum == 0 {
			tempSkip.Insert(string(keyBytes),value)
		}
	}
	iu := &immuMemUnit{sl: tempSkip,walFileName: walFileName}
	immuChannel <- iu
	return true
}


// CheckImmuMemList 后台线程对不可变内存表进行持久化
func CheckImmuMemList() {
	for sl := range immuChannel {
		if sl == nil {
			continue
		}
		MemListFlush(gbsp.sst, gbsp.mt, sl)
	}
}


