package core

import (
	"io/ioutil"
	"os"
	"path"
	"someIndex/src/data_structure"
	"someIndex/src/log"
	"someIndex/src/util"
	"strconv"
	"sync"
)

const (
	MaxTableLevel      = 16
	FirstMaxTableCount = int64(10)
	DbDataFileCoreName = "let_block"
	DbDataFileExName = ".let"
)

const (
	TypeByteSize = int64(8)
)

// SsTableTree sstable的维护数据结构
type SsTableTree struct {
	//当前的层数
	tableLevel int
	//内存中的sstable头
	sstableHeads [MaxTableLevel]*SsTableTreeNode
	//各层级的table max数量
	maxTableCounts [MaxTableLevel]int64
	//各层级当前的数量
	tableCounts [MaxTableLevel]int64
	//数据库目录
	Dir string
	//🔒
	mm *sync.Mutex
	//同步队列
	mg *sync.WaitGroup
}

type SsTableTreeNode struct {
	msst *MemSsTable
	next *SsTableTreeNode
	front *SsTableTreeNode
	tail *SsTableTreeNode
}

func NewSsTableTree(dir string) *SsTableTree {
	if dir == "" {
		return nil
	}
	arr := [MaxTableLevel]*SsTableTreeNode{}
	for i:=0;i<len(arr);i++ {
		arr[i] = &SsTableTreeNode{next: nil,tail: nil,msst: nil}
	}
	maxArr := [MaxTableLevel]int64{}
	temp := FirstMaxTableCount
	for i:=0;i<len(maxArr);i++ {
		maxArr[i] = temp
		temp *= 2
	}
	ss := SsTableTree{tableLevel: 0,sstableHeads: arr,maxTableCounts: maxArr, Dir: dir,mm: &sync.Mutex{},mg: &sync.WaitGroup{}}
	return &ss
}

// AddMsstNode 添加内存sstable到数据结构中
func (sst *SsTableTree) AddMsstNode(msst *MemSsTable,level int) {
	if msst == nil {
		return
	}
	if level < 0 || level >= MaxTableLevel {
		return
	}
	head := sst.sstableHeads[level]
	sstNode := &SsTableTreeNode{msst: msst}
	if head.next == nil {
		head.next = sstNode
		head.tail = sstNode
		sstNode.front = head
	}else {
		//头插法，更新的节点放在更前面
		sstNode.next = head.next
		head.next.front = sstNode
		head.next = sstNode
		sstNode.front = head
	}
	sst.tableCounts[level]++
}

func (sst *SsTableTree) RemoveTailNode(tarLevel int)  {
	if tarLevel < 0 || tarLevel >= MaxTableLevel {
		return
	}
	if sst.tableCounts[tarLevel] == 0 {
		return
	}
	head := sst.sstableHeads[tarLevel]
	tar := head.tail
	temp := tar.front
	temp.next = nil
	tar.front = nil
	head.tail = temp
	sst.tableCounts[tarLevel]--
}



// SsTableTreeSearch 搜索磁盘
func (sst *SsTableTree) SsTableTreeSearch(key string) []byte {
	//如果当前层节点不为0则从头开始扫描
	for curLevel := 0; curLevel < MaxTableLevel || sst.tableCounts[curLevel] != 0;curLevel++{
		head := sst.sstableHeads[curLevel]
		cur := head.next
		for cur != nil {
			msst := cur.msst
			areaMap := msst.IndexInfoAreaMap
			if v,has := areaMap[key]; has {
				if v.IsDel == 1 {
					//被删除了
					return nil
				}
				index := 16 + msst.MetaArea.IndexInfoAreaLen + v.Start
				apath := path.Join(sst.Dir,DbDataFileCoreName + msst.seq + DbDataFileExName)
				value := ssTableTreeSearch_sstableTreeSearchRead(apath,index)
				if value != nil {
					return value
				}
			}
			cur = cur.next
		}
	}
	return nil
}


// MemListFlush 将内存表合并入磁盘中
func MemListFlush(sst *SsTableTree,mt *MemTable,iu *immuMemUnit) {
	if iu == nil || iu.sl == nil || iu.walFileName == "" {
		return
	}
	//获取数据集
	datas := iu.sl.Traverse()
	msst := memListFlush_convert(datas)
	//拼接序列号，由层数+层中序号
	head := sst.sstableHeads[0]
	var temp string
	if head.next == nil {
		temp = "01"
	}else {
		atoi,_ := strconv.Atoi(head.next.msst.seq[2:])
		temp = util.GetNumStr_two(int64(atoi + 1))
	}
	seq := util.GetNumStr_two(0) + temp
	msst.seq = seq
	sst.AddMsstNode(msst,0)
	memListFlush_persistence(msst,datas,sst.Dir)
	mt.GetWal().RemoveImmuLogFile(iu.walFileName)
}

// memListFlush_convert 将内存表转化成sstable
func memListFlush_convert(datas []*data_structure.Data) *MemSsTable {
	//数据区长度
	dataAreaLen := int64(0)
	//索引区长度
	indexAreaLen := int64(0)
	msst := MemSsTable{}
	indexInfoMap := make(map[string]*IndexInfoAreaUnit)

	//计算索引区长度
	//计算数据区长度
	for _,data := range datas {
		key := data.Key
		keyBytes := []byte(key)
		value := data.Value
		del := data.IsDel
		//将索引数据存入内存的数据结构
		ia := IndexInfoAreaUnit{Start: dataAreaLen}
		//叠加数据区长度
		temp := 8 + int64(len(keyBytes)) + 8 + int64(len(value)) + 1
		dataAreaLen += temp
		//叠加索引区长度
		indexAreaLen += 8 + int64(len(keyBytes)) + 8 + 1
		if del {
			ia.IsDel = 1
		}else {
			ia.IsDel = 0
		}
		indexInfoMap[key] = &ia
	}
	//创建元数据区
	ma := MetaArea{DataAreaLen: dataAreaLen,IndexInfoAreaLen: indexAreaLen}
	//封装table
	msst.MetaArea = &ma
	msst.IndexInfoAreaMap = indexInfoMap
	return &msst
}

// memListFlush_persistence 将内存中的table持久化到磁盘
func memListFlush_persistence(msst *MemSsTable,datas []*data_structure.Data, dir string) {
	file,err := os.OpenFile(path.Join(dir, DbDataFileCoreName+ msst.seq +DbDataFileExName),os.O_RDWR|os.O_CREATE|os.O_APPEND,0666)
	if err != nil {
		return
	}
	defer util.MyCloseFile(file)
	//写元数据区，一个是索引区长度，一个是数据区长度
	util.Int64FileWrite(file,msst.MetaArea.IndexInfoAreaLen)
	util.Int64FileWrite(file,msst.MetaArea.DataAreaLen)

	//写索引区
	for k,v := range msst.IndexInfoAreaMap {
		keyBytes := []byte(k)
		//写入kl
		util.Int64FileWrite(file,int64(len(keyBytes)))
		//写入key
		util.BytesFileWrite(file,keyBytes)
		//写入key对应的起始位置
		util.Int64FileWrite(file,int64(v.Start))
		//写入是否被删除
		util.Int8FileWrite(file,int8(v.IsDel))
	}

	//写数据区
	for _,data := range datas {
		key := data.Key
		keyBytes := []byte(key)
		value := data.Value
		//写入kl
		util.Int64FileWrite(file,int64(len(keyBytes)))
		//写入key
		util.BytesFileWrite(file,keyBytes)
		//写入vl
		util.Int64FileWrite(file, int64(len(value)))
		//写入value
		util.BytesFileWrite(file,value)
		//写入是否被删除
		var temp int8
		if data.IsDel {
			temp = 1
		}else {
			temp = 0
		}
		util.Int8FileWrite(file,temp)
	}
}

// RecoverFromFile 从sstable文件中恢复
func (sst *SsTableTree) RecoverFromFile() {
	//获取数据库目录
	dbDir := sst.Dir
	//扫描后缀为let的文件
	dd, err := ioutil.ReadDir(dbDir)
	if err != nil {
		return
	}
	for _,fi := range dd {
		if fi.IsDir() {
			continue
		}
		ext := path.Ext(fi.Name())
		if ext != DbDataFileExName || len(fi.Name()) < 8 {
			continue
		}
		fileName := fi.Name()
		apath := path.Join(sst.Dir,fileName)
		//解析层数
		sstLevel,_ := strconv.Atoi(fileName[len(fileName) - 8 : len(fileName) - 6])
		//解析序列号
		seq := fileName[len(fileName) - 8 : len(fileName) - 4]
		msst := recoverFromFile_recoverSstableTreeRead(apath)
		msst.seq = seq
		sst.AddMsstNode(msst,sstLevel)
	}
}

func recoverFromFile_recoverSstableTreeRead(path string) *MemSsTable {
	file, bo := util.MyOpenFile_RCA(path)
	if !bo {
		return nil
	}
	defer util.MyCloseFile(file)
	msst := &MemSsTable{}
	//读取元数据区，第一个8字节是索引区长度，第二个是数据区长度
	indexAreaLen, b1 := util.Int64FileRead(file)
	if !b1 {
		return nil
	}
	dataAreaLen, b2 := util.Int64FileRead(file)
	if !b2 {
		return nil
	}
	ma := &MetaArea{IndexInfoAreaLen: indexAreaLen,DataAreaLen: dataAreaLen}
	msst.MetaArea = ma
	//读取索引区，并将其封装到msst中
	run := int64(0)
	tempMap := make(map[string]*IndexInfoAreaUnit)
	for run < indexAreaLen {
		//读取kl
		kl, bo := util.Int64FileRead(file)
		if !bo {
			log.EasyInfoLog("恢复数据有错误")
			break
		}
		run += 8
		//读取key
		keyBytes, b1 := util.BytesFileRead(file, kl)
		if !b1 {
			log.EasyInfoLog("恢复数据有错误")
			break
		}
		run += kl
		//读取start
		start, b2 := util.Int64FileRead(file)
		if !b2 {
			log.EasyInfoLog("恢复数据有错误")
			break
		}
		run += 8
		//读取操作类型
		acType, b3 := util.Int8FileRead(file)
		if !b3 {
			log.EasyInfoLog("恢复数据有错误")
			break
		}
		run += 1
		iu := &IndexInfoAreaUnit{Start: start,IsDel: acType}
		tempMap[string(keyBytes)] = iu
	}
	msst.IndexInfoAreaMap = tempMap
	return msst
}


func ssTableTreeSearch_sstableTreeSearchRead(path string,index int64) []byte {
	file,bo := util.MyOpenFile_RCA(path)
	if !bo {
		return nil
	}
	defer util.MyCloseFile(file)
	_, err := file.Seek(index, 0)
	if err != nil {
		log.EasyInfoLog("偏移出错")
		return nil
	}
	//读取kl
	kl, b1 := util.Int64FileRead(file)
	if !b1 {
		return nil
	}

	//读取key
	_, b2 := util.BytesFileRead(file, kl)
	if !b2 {
		return nil
	}
	//读取vl
	vl, b3 := util.Int64FileRead(file)
	if !b3 {
		return nil
	}
	//读取value
	value, b4 := util.BytesFileRead(file, vl)
	if !b4 {
		return nil
	}
	//读取操作类型
	_,b5 := util.Int8FileRead(file)
	if !b5 {
		return nil
	}
	return value
}