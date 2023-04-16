package data_structure

import (
	"someIndex/src/util"
)

// Insert 插入数据
func (sl *SkipList) Insert(key string,value []byte) (*InsertResultVO,bool) {
	if key == "" || len(value) == 0 {
		return &InsertResultVO{code: FAILED,msg: "key或者value为空"},false
	}
	//获取当前高度
	curLevel := sl.Level
	//需要生成的索引高度
	needLevel := RandomLevel()
	//将当前最大层数重置
	sl.Level = util.GetMax(curLevel,needLevel)
	curLevel = sl.Level
	//获取对应高度的头节点
	head := sl.LevelHeads[curLevel]
	//前驱
	var front = head
	//当前
	var current = front.Next
	//上一层的节点
	var up *DataSkipListNode = nil
	//将要插入的数据
	data := &Data{Key: key,Value: value,IsDel: false}
	isInsert := true
	for i:=curLevel;i>=0;i-- {
		for current != nil {
			if key > current.Data.Key {
				front = front.Next
				current = current.Next
				continue
			}else if key == current.Data.Key {
				current.Data.Value = value
				current.Data.IsDel = false
				isInsert = false
				if up != nil {
					up.Down = front.Next
					up = front.Next
				}
				return &InsertResultVO{code: NONE,msg: "Insert none !"},true
			}else {
				break
			}
		}

		//插入节点
		if i <= needLevel {
			front.Next = &DataSkipListNode{Data: data,Next: current,Down: nil}
			if up == nil {
				up = front.Next
			}else {
				up.Down = front.Next
				up = front.Next
			}
		}

		//往下走
		if front.Down != nil {
			front = front.Down
			current = front.Next
		}
	}
	if isInsert {
		sl.NodeCount++
		sl.OccupiedBytes += int64(len(value))
		sl.OccupiedBytes += int64(len([]byte(key)))
	}
	return &InsertResultVO{code: SUCCESS,msg: "Insert success !"},true
}


// Search 搜索
func (sl *SkipList) Search(key string) (*SearchResultVO,bool) {
	if key == "" {
		return &SearchResultVO{Code: FAILED, Msg: "key can not space"},false
	}
	curLevel := sl.Level
	head := sl.LevelHeads[curLevel]
	//前驱
	var front = head
	//当前
	var current = front.Next
	for i:=curLevel;i>=0;i-- {
		for current != nil {
			if key > current.Data.Key {
				current = current.Next
				front = front.Next
			}else if key < current.Data.Key {
				break
			}else {
				if current.Data.IsDel {
					return &SearchResultVO{Code: DELETED, Msg: "search deleted !"},true
				}else {
					return &SearchResultVO{Code: SUCCESS, Msg: "search success !", Value: current.Data.Value},true
				}
			}
		}

		if front.Down != nil {
			front = front.Down
			current = front.Next
		}
	}
	return &SearchResultVO{Code: NONE, Msg: "search none !"},false
}

// Delete 删除
func (sl *SkipList) Delete(key string) bool {
	if key == "" {
		return false
	}
	curLevel := sl.Level
	head := sl.LevelHeads[curLevel]
	var front = head
	var cur = front.Next
	for i:=curLevel;i>=0;i-- {
		for cur != nil {
			if key > cur.Data.Key {
				front = front.Next
				cur = cur.Next
			}else if key < cur.Data.Key {
				break
			}else {
				cur.Data.IsDel = true
				return true
			}
		}

		if front.Down != nil {
			front = front.Down
			cur = front.Next
		}
	}
	return false
}

// Traverse 遍历
func (sl *SkipList) Traverse() []*Data {
	datas := make([]*Data,0)
	run := sl.LevelHeads[0].Next
	for run != nil {
		datas = append(datas,run.Data)
		run = run.Next
	}
	return datas
}