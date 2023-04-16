package main

import (
	"someIndex/src/core"
)

type Integer struct {
	Val int32
}

func main() {

	strap := core.BootStrap{}
	strap.GlobalInit("E:\\someIndex\\test\\", 5557)

}
