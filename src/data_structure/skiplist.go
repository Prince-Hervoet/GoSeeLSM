package data_structure

import (
	"crypto/rand"
	"math/big"
	"sync"
)

const (
	// RandomRange 生成随机数范围
	RandomRange = int64(12)
	// LevelMax 跳表索引层数最大值
	LevelMax = 64
)

const (
	SUCCESS = 10
	FAILED = -1
	NONE = 0
	DELETED = -2
)

type SearchResultVO struct {
	Code  int
	Value []byte
	Msg   string
}

type InsertResultVO struct {
	code int
	msg string
}


// SkipList 跳表结构
type SkipList struct {
	//当前最大的索引层数
	Level int
	//索引链头节点集合
	LevelHeads [LevelMax + 1]*DataSkipListNode
	//原生数据链节点数量
	NodeCount int
	//目前所占用的字节数
	OccupiedBytes int64
	//🔒
	mm *sync.Mutex
}


// DataSkipListNode 核心数据节点
type DataSkipListNode struct {
	//数据的结构
	Data *Data
	//下一个节点
	Next *DataSkipListNode
	//下一层节点
	Down *DataSkipListNode
}


// Data 数据
type Data struct {
	//键
	Key string
	//数据
	Value []byte
	//标记删除，true表示被删除
	IsDel bool
}

// NewSkipList 获取一个跳表结构
func NewSkipList() *SkipList {
	//初始化头节点集合
	arr := [LevelMax + 1]*DataSkipListNode{}
	var frontDown *DataSkipListNode = nil
	for i:=0;i<len(arr);i++ {
		temp := &DataSkipListNode{Data: nil, Next: nil, Down: nil}
		arr[i] = temp
		if frontDown == nil {
			frontDown = temp
		}else {
			temp.Down = frontDown
			frontDown = temp
		}
	}
	list := &SkipList{Level: 0, LevelHeads: arr, NodeCount: 0,mm: &sync.Mutex{}}
	return list
}

// RandomLevel 生成随机层数
func RandomLevel() int {
	curLevel := 0
	num := int64(-1)
	standard := RandomRange / 3
	res,_ := rand.Int(rand.Reader,big.NewInt(RandomRange))
	num = res.Int64()
	for num <= standard {
		res,_ = rand.Int(rand.Reader,big.NewInt(RandomRange))
		curLevel += 1
		num = res.Int64()
	}
	if curLevel >= LevelMax {
		return LevelMax
	}
	return curLevel
}



