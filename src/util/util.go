package util

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
)

var lock = sync.Mutex{}
var frontUnix = int64(0)

// ObjectToBytes 将对象转化成字节数组
func ObjectToBytes(v interface{}) []byte {
	marshal, err := json.Marshal(v)
	fmt.Println(string(marshal))
	if err != nil {
		return nil
	}
	return marshal
}


func BytesToObject(v interface{},bs []byte) {
	err := json.Unmarshal(bs, v)
	if err != nil {
		return
	}
}

// GetMax 整型比较大小
func GetMax(a,b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func GetNumStr_two(num int64) string {
	if num < 10 {
		return "0" + strconv.FormatInt(num,10)
	}else {
		return strconv.FormatInt(num,10)
	}
}

