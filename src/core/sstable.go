package core

//总体结构为：元数据区 + 索引区 + 数据区
//元数据区包含整个sstale中索引区和数据区的下标
//索引区包含数据区中数据的下标
//磁盘中元数据区本身占用 2 * 8 = 16 字节

// MemSsTable 内存中的sstable
type MemSsTable struct {
	//元数据区
	MetaArea *MetaArea
	//索引区
	IndexInfoAreaMap map[string]*IndexInfoAreaUnit
	//table的序列号
	seq string
}

// MetaArea 元数据区
type MetaArea struct {
	//索引区域的长度
	IndexInfoAreaLen int64
	//数据区域的长度
	DataAreaLen int64
}

// IndexInfoAreaUnit 索引区
type IndexInfoAreaUnit struct {
	//键值对的开始
	Start int64
	//当前是否被删除了
	IsDel int8
}

func NewMemSsTable() *MemSsTable {
	return &MemSsTable{}
}


