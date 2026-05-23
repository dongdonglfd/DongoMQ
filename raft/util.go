package raft

import (
	"fmt"
	"log"
	"path"
	"runtime"
	"strconv"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	_, file, lineNo, ok := runtime.Caller(1) //获取调用者的信息，文件名和行号
	if !ok {
		log.Println("runtime.Caller() failed")
	}

	filename := path.Base(file) //获取调用该函数的源文件的基础名称

	if Debug {
		mu.Lock()
		format = filename + ":" + strconv.Itoa(lineNo) + ": " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}
