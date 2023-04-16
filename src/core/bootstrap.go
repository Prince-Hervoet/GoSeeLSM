package core

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

var gbsp *BootStrap

type BootStrap struct {
	mt *MemTable
	sst *SsTableTree
}


type ResultVO struct {
	code int
	msg string
	data interface{}
}

// GlobalInit 全局初始化
func (bsp *BootStrap) GlobalInit(dir string,port int)  {
	//获取一个内存表
	mt := NewMemTable(dir)
	//恢复内存表
	mt.RecoverFromFile()
	//获取一个内存维护树
	sst := NewSsTableTree(dir)
	//恢复内存维护树
	sst.RecoverFromFile()
	//search := sst.SsTableTreeSearch("9988")
	//fmt.Println(string(search))
	Compaction(sst,0)
	//启动器赋值
	bsp.mt = mt
	bsp.sst = sst
	gbsp = bsp
	go CheckImmuMemList()
	//启动gin框架
	dbStart(bsp.mt,port)

}

//将接口绑定端口
func dbStart(mt *MemTable,port int) {
	gg := gin.Default()
	gg.POST("/dbadd", func(context *gin.Context) {
		key := context.PostForm("key")
		value := context.PostForm("value")
		mt.Add(key,[]byte(value))
		context.JSON(http.StatusOK,"add success !")
	})
	gg.GET("/dbsearch", func(context *gin.Context) {
		key := context.Query("key")
		search := mt.Search(key)
		context.JSON(http.StatusOK,string(search))
	})
	gg.PUT("/dbdel", func(context *gin.Context) {
		key := context.Query("key")
		mt.Delete(key)
		context.JSON(http.StatusOK,"delete success !")
	})
	gg.Run(":" + strconv.Itoa(port))
}


