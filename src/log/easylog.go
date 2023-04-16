package log

import (
	"fmt"
	"time"
)

func EasyInfoLog(msg string) {
	fmt.Println(time.Now().String() + " [INFO] " + msg)
}
