package core

import (
	"fmt"
	"os"
	"path"
	"someIndex/src/data_structure"
	"someIndex/src/util"
)

const (
	Synthetic = 4
)

type keyValueBlock struct {
	key string
	isDel int8
}

// Compaction 将sstable文件进行压实
func Compaction(sst *SsTableTree,tarLevel int) {
	if tarLevel < 0 || tarLevel >= MaxTableLevel {
		return
	}
	datas := merge(sst, tarLevel)
	if datas == nil {
		return
	}
	//创建一个临时文件let_blockxx.let
	apath := path.Join(sst.Dir,DbDataFileCoreName + "xx" + DbDataFileExName)
	file,bo := util.MyOpenFile_RCA(apath)
	if !bo {
		return
	}
	defer util.MyCloseFile(file)

	//遍历，计算
	dataAreaLen := int64(0)
	indexAreaLen := int64(0)
	tempMap := make(map[string]*IndexInfoAreaUnit)
	for _,data := range datas {
		key := data.Key
		value := data.Value
		isDel := data.IsDel
		keyBytes := []byte(key)
		ia := &IndexInfoAreaUnit{Start: dataAreaLen}
		if isDel {
			ia.IsDel = 1
		}else {
			ia.IsDel = 0
		}

		//叠加数据区长度
		temp := 8 + 8 + len(keyBytes) + len(value) + 1
		dataAreaLen += int64(temp)

		//叠加索引区长度
		indexAreaLen += 8 + int64(len(keyBytes)) + 8 + 1
		tempMap[key] = ia
	}
	//设置元数据区
	ma := &MetaArea{DataAreaLen: dataAreaLen,IndexInfoAreaLen: indexAreaLen}

	//开始写入数据

	//写入元数据区
	util.Int64FileWrite(file,ma.IndexInfoAreaLen)
	util.Int64FileWrite(file,ma.DataAreaLen)

	//写入索引数据
	for k,v := range tempMap {
		keyBytes := []byte(k)
		//写入kl
		util.Int64FileWrite(file,int64(len(keyBytes)))
		//写入key
		util.BytesFileWrite(file,keyBytes)
		//写入start
		util.Int64FileWrite(file,v.Start)
		//写入是否被删除
		util.Int8FileWrite(file,v.IsDel)
	}

	//写入数据区
	for _,data := range datas {
		key := data.Key
		keyBytes := []byte(key)
		value := data.Value
		isDel := data.IsDel
		//写入kl
		util.Int64FileWrite(file,int64(len(keyBytes)))
		//写入key
		util.BytesFileWrite(file,keyBytes)
		//写入vl
		util.Int64FileWrite(file,int64(len(value)))
		//写入value
		util.BytesFileWrite(file,value)
		//写入是否被删除
		if isDel {
			util.Int8FileWrite(file,1)
		}else {
			util.Int8FileWrite(file,0)
		}
	}

	//将当前合并完成的文件节点添加到下一层的结构上
	seq := util.GetNumStr_two(int64(tarLevel) + 1) + util.GetNumStr_two(sst.tableCounts[tarLevel+1] + 1)
	msst := &MemSsTable{MetaArea: ma,IndexInfoAreaMap: tempMap,seq: seq}
	sst.AddMsstNode(msst,tarLevel + 1)

	//删除原来的文件
	//获取目标层的头节点
	head := sst.sstableHeads[tarLevel]
	//从后往前合并
	//old
	behind := head.tail
	//new
	cur := behind.front
	if cur.front != head {
		temp := cur.front
		//如果前面一个不是头节点，就删除两个节点并将前面的节点的序号置为xxx1
		temp.msst.seq = util.GetNumStr_two(int64(tarLevel)) + util.GetNumStr_two(1)
	}
	sst.RemoveTailNode(tarLevel)
	sst.RemoveTailNode(tarLevel)
	os.Remove(path.Join(sst.Dir,DbDataFileCoreName + behind.msst.seq + DbDataFileExName))
	os.Remove(path.Join(sst.Dir,DbDataFileCoreName + cur.msst.seq + DbDataFileExName))
	util.MyCloseFile(file)
	os.Rename(apath, path.Join(sst.Dir, DbDataFileCoreName+seq+DbDataFileExName))
	fmt.Println(path.Join(sst.Dir, DbDataFileCoreName+seq+DbDataFileExName))
}




// Merge 合并多个sstable
func merge(sst *SsTableTree,tarLevel int) []*data_structure.Data {
	//获取目标层的头节点
	head := sst.sstableHeads[tarLevel]
	//从后往前合并
	//old
	behind := head.tail
	if behind == nil {
		return nil
	}
	//new
	cur := behind.front
	if cur == nil {
		return nil
	}
	if cur == head {
		//如果已经是头节点，就直接退出
		return nil
	}
	//获取后面节点的序号
	oldSeq := behind.msst.seq
	//获取当前节点的序号
	newSeq := cur.msst.seq
	path1 := path.Join(sst.Dir,DbDataFileCoreName + oldSeq + DbDataFileExName)
	path2 := path.Join(sst.Dir,DbDataFileCoreName + newSeq + DbDataFileExName)
	//打开旧版本文件
	oldFile,bo := util.MyOpenFile_RONLY(path1)
	if !bo {
		return nil
	}
	defer util.MyCloseFile(oldFile)

	//打开新版本文件
	newFile,b1 := util.MyOpenFile_RONLY(path2)
	if !b1 {
		return nil
	}
	defer util.MyCloseFile(newFile)

	stat1, err := newFile.Stat()
	stat2, err := oldFile.Stat()
	if err != nil {
		return nil
	}
	//新版本文件大小
	newFileSize := stat1.Size()
	//旧版本文件大小
	oldFileSize := stat2.Size()

	if newFileSize == 0 || oldFileSize == 0 {
		return nil
	}

	run1 := int64(0)
	run2 := int64(0)

	//先读取新版本文件的元数据区
	indexAreaLen1, b3 := util.Int64FileRead(newFile)
	d1, b4 := util.Int64FileRead(newFile)
	if !b3 || !b4 {
		return nil
	}
	index1 := 16 + indexAreaLen1
	_, err = newFile.Seek(index1, 0)
	if err != nil {
		return nil
	}

	//读取旧版本文件的元数据区
	indexAreaLen2, b5 := util.Int64FileRead(oldFile)
	d2, b6 := util.Int64FileRead(oldFile)
	if !b5 || !b6 {
		return nil
	}
	index2 := 16 + indexAreaLen2
	_, err = oldFile.Seek(index2, 0)
	if err != nil {
		return nil
	}

	//开始合并
	//加入新的索引
	sl := data_structure.NewSkipList()
	flag := 0
	var key1,key2 string
	var value1,value2 []byte
	var tNum1,tNum2 int8
	for run1 < d1 && run2 < d2 {

		if flag == 0 {
			key1, value1,tNum1 = merge_read(newFile, &run1)
			if key1 == "" {
				return nil
			}
			key2, value2,tNum2 = merge_read(oldFile, &run2)
			if key2 == "" {
				return nil
			}
		}else if flag == 1 {
			key1, value1,tNum1 = merge_read(newFile, &run1)
			if key1 == "" {
				return nil
			}
		}else if flag == -1 {
			key2, value2,tNum2 = merge_read(oldFile, &run2)
			if key2 == "" {
				return nil
			}
		}

		//如果新版本文件中的key小，那么直接加入
		//如果旧版本文件中的key小，也直接加入
		//如果相等，则加入新版本的value
		if key1 < key2 {
			if tNum1 == 0 {
				sl.Insert(key1,value1)
				key1 = ""
				value1 = nil
			}
			flag = 1
		}else if key1 > key2 {
			if tNum2 == 0 {
				sl.Insert(key2,value2)
				key2 = ""
				value2 = nil
			}
			flag = -1
		}else {
			if tNum1 == 0 {
				sl.Insert(key1,value1)
				key1 = ""
				value1 = nil
			}
			flag = 0
		}
	}

	for run1 < d1 {
		key1, value1,tNum1 = merge_read(newFile, &run1)
		if tNum1 == 0 {
			sl.Insert(key1,value1)
		}
	}

	for run2 < d2 {
		key2, value2,tNum2 = merge_read(oldFile, &run2)
		if tNum2 == 0 {
			sl.Insert(key2,value2)
		}
	}
	if key1 != "" {
		if tNum1 == 0 {
			sl.Insert(key1,value1)
		}
	}
	if key2 != "" {
		if tNum2 == 0 {
			sl.Insert(key2,value2)
		}
	}
	fmt.Println("asdfasdfasdfasdfasdfasdfasdfasdf")
	return sl.Traverse()
}

func merge_read(file *os.File,run *int64) (string,[]byte,int8) {
	//读取新版本的kl
	akl, bo := util.Int64FileRead(file)
	if !bo {
		return "",nil,1
	}
	*run += 8
	//读取新版本的key
	fmt.Println(akl)
	akeyBytes,b1 := util.BytesFileRead(file,akl)
	if !b1 {
		return "",nil,1
	}
	*run += akl
	//读取新版本的vl
	avl,b2 := util.Int64FileRead(file)
	if !b2 {
		return "",nil,1
	}
	*run += 8
	//读取新版本的value
	value,b3 := util.BytesFileRead(file,avl)
	if !b3 {
		return "",nil,1
	}
	*run += avl
	//读取操作类型
	tNum,b4 := util.Int8FileRead(file)
	if !b4 {
		return "",nil,1
	}
	*run += 1
	return string(akeyBytes),value,tNum
}
