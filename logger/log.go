package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// 从环境变量中获取日志的详细程度
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity level: %s", v)
		}
	}
	return level
}

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var mu sync.Mutex
var debug bool = true
var debug_raft bool = true

func LOGinit() {
	mu.Lock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	mu.Unlock()
}

func DEBUG(topic logTopic, format string, a ...interface{}) {
	_, file, lineNo, ok := runtime.Caller(1) //获取调用者的信息，文件名和行号
	if !ok {
		log.Println("runtime.Caller() failed")
	}

	filename := path.Base(file) //获取调用该函数的源文件的基础名称

	if debug {
		mu.Lock()
		prefix := fmt.Sprintf("%v ", string(topic))
		format = prefix + filename + ":" + strconv.Itoa(lineNo) + ": " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}

func DEBUG_RAFT(topic logTopic, format string, a ...interface{}) {
	_, file, lineNo, ok := runtime.Caller(1) //获取调用者的信息，文件名和行号
	if !ok {
		log.Println("runtime.Caller() failed")
	}

	filename := path.Base(file) //获取调用该函数的源文件的基础名称

	if debug_raft {
		mu.Lock()
		prefix := fmt.Sprintf("%v ", string(topic))
		format = prefix + filename + ":" + strconv.Itoa(lineNo) + ": " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}
