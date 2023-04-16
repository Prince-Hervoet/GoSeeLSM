package test

import (
	"crypto/rand"
	"math/big"
)

type Skiplist struct {
	heads [65]*node
	level int
}

type node struct {
	val int
	next *node
	down *node
}

// RandomLevel 生成随机层数
func randomLevel() int {
	curLevel := 0
	var le int64 = -1
	var half int64 = 1
	for le == -1 || le > half {
		if le != -1 {
			curLevel++
		}
		if curLevel == 32 {
			break
		}
		n, _ := rand.Int(rand.Reader, big.NewInt(8))
		le = n.Int64()
	}
	return curLevel
}


func Constructor() Skiplist {
	arr := [65]*node{}
	var frontDown *node = nil
	for i:=0;i<len(arr);i++ {
		arr[i] = &node{}
		if frontDown == nil {
			frontDown = arr[i]
		}else {
			arr[i].down = frontDown
			frontDown = arr[i]
		}
	}
	return Skiplist{heads: arr}
}


func (this *Skiplist) Search(target int) bool {
	curLevel := this.level
	head := this.heads[curLevel]
	var front = head
	var cur = front.next
	for i:=curLevel;i>=0;i-- {
		for cur != nil {
			if target > cur.val {
				front = front.next
				cur = cur.next
			}else if target == cur.val {
				return true
			}else {
				break
			}
		}

		front = front.down
		cur = front.next
	}
	return false
}


func (this *Skiplist) Add(num int)  {
	curLevel := this.level
	need := randomLevel()
	this.level = getMax(curLevel,need)
	head := this.heads[curLevel]
	var front = head
	var cur = front.next
	var up *node = nil
	for i:=curLevel;i>=0;i-- {
		for cur != nil {
			if num > cur.val {
				front = front.next
				cur = cur.next
			}else {
				break
			}
		}

		if need >= i {
			will := &node{val: num}
			front.next = will
			will.next = cur
			if up == nil {
				up = will
			}else {
				up.down = will
				up = will
			}
		}

		if front.down != nil {
			front = front.down
			cur = front.next
		}

	}

}

func getMax(a,b int) int {
	if a >= b {
		return a
	}else {
		return b;
	}
}


func (this *Skiplist) Erase(num int) bool {
	curLevel := this.level
	head := this.heads[curLevel]
	isOk := false
	var front = head
	var cur = front.next
	for i:=curLevel;i>=0;i-- {
		for cur != nil {
			if num > cur.val {
				front = front.next
				cur = cur.next
			}else if num < cur.val {
				break
			}else {
				temp := cur.next
				front.next = temp
				cur.next = nil
				isOk = true
				break
			}
		}

		if front.down != nil {
			front = front.down
			cur = front.next
		}
	}

	return isOk
}
