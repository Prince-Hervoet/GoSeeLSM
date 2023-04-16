package util

import "time"

var timestampMap = make(map[string]int64)

func SetTimeUtil(key string) bool {
	if key == "" {
		return false
	}
	timestampMap[key] = time.Now().UnixMilli()
	return true
}


func GetTimeUtil(key string) float64 {
	now := time.Now().UnixMilli()
	last := timestampMap[key]
	var cha float64
	cha = (float64(now) - float64(last)) / float64(1000)
	delete(timestampMap,key)
	return cha
}
