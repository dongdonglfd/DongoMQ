package server

import (
	"DongoMQ/logger"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type File struct {
	mu        sync.RWMutex
	filename  string
	node_size int
}

// 分块存储
type Key struct {
	Size        int64 `json:"size"`
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
}

type Message struct {
	Index      int64  `json:"index"`
	Size       int8   `json:"size"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

// 先检查该磁盘是否存在该文件，如不存在则需要创建
func NewFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {
		fd, err = CreateFile(path_name)
		if err != nil {
			Err = "Create file failed"
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return nil, nil, Err, err
		}
	} else {
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "Open file failed"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
}

func CheckFile(path string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path) {
		Err = "NotFile"
		err = errors.New(Err)
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, nil, Err, err
	} else {
		fd, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "Open file failed"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path,
		node_size: NODE_SIZE,
	}

	logger.DEBUG(logger.DLog, "the file is %v\n", file)

	return file, fd, "ok", err
}

// 修改文件名
func (f *File) Update(path, fileName string) error {
	OldFilePath := f.filename
	NewFilePath := path + "/" + fileName
	f.mu.Lock()
	f.filename = NewFilePath
	f.mu.Unlock()

	return MovName(OldFilePath, NewFilePath)
}

// 读取文件，获取该partition的最后一个index
func (f *File) GetIndex(file *os.File) int64 {
	var node Key
	var index, offset int64
	index = -1
	offset = 0
	data_node := make([]byte, NODE_SIZE)
	f.mu.RLock()
	defer f.mu.RUnlock()

	for {
		// 读取节点头
		_, err := file.ReadAt(data_node, offset)

		if err == io.EOF {
			//读到文件末尾
			break
		} else if err != nil {
			// 其他读取错误
			return -1
		}
		// 解析节点
		if err := binary.Read(bytes.NewReader(data_node), binary.BigEndian, &node); err != nil {
			return -1
		}

		index = node.End_index
		offset += NODE_SIZE + node.Size
	}

	return index
}

func (f *File) WriteFile(file *os.File, node Key, data_msg []byte) bool {
	data_node := &bytes.Buffer{}
	err := binary.Write(data_node, binary.BigEndian, node)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		logger.DEBUG(logger.DError, "%v turn bytes fail\n", node)
		return false
	}

	f.mu.Lock()

	if f.node_size == 0 {
		f.node_size = len(data_node.Bytes())
	}
	if _, err = file.Write(data_node.Bytes()); err != nil {
		return false
	}
	
	if _, err = file.Write(data_msg); err != nil {
		return false
	}

	f.mu.Unlock()

	if err != nil {
		return false
	} else {
		return true
	}
}

func (f *File) OpenFileRead() *os.File {
	f.mu.RLock()
	fd, err := os.OpenFile(f.filename, os.O_RDONLY, 0666)
	f.mu.RUnlock()
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil
	}
	return fd
}

func (f *File) GetFirstIndex(file *os.File) int64 {
	var node Key
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	_, err := file.ReadAt(data_node, 0)

	if err == io.EOF {
		//读到文件末尾
		logger.DEBUG(logger.DLeader, "read All file,the first_index is %v\n", 0)
		return 0
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)

	return node.Start_index
}

func (f *File) FindOffset(file *os.File, index int64) (int64, error) {
	data_node := make([]byte, NODE_SIZE)
	offset := int64(0)
	for {
		logger.DEBUG(logger.DLog, "the file name is %v\n", f.filename)
		f.mu.RLock()
		size, err := file.ReadAt(data_node, offset)
		f.mu.RUnlock()

		//错误处理
		if err != nil {
			if err == io.EOF {
				logger.DEBUG(logger.DLog, "Reached EOF without finding index %d\n", index)
				return index, io.EOF
			}
			logger.DEBUG(logger.DLog2, "Read error: %v", err)
			return -1, fmt.Errorf("read error: %w", err)
		}

		if size != NODE_SIZE {
			logger.DEBUG(logger.DLog2, "Incomplete read: got %d bytes,expected %d\n", size, NODE_SIZE)
			return -1, fmt.Errorf("incomplete node read:expected %d bytes,got %d", NODE_SIZE, size)
		}

		//二进制解析
		var node Key
		if err := binary.Read(bytes.NewReader(data_node), binary.BigEndian, &node); err != nil {
			logger.DEBUG(logger.DLog2, "Binary read error:%v", err)
			return -1, fmt.Errorf("binary read error: %w", err)
		}

		//索引比较
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size)
			continue
		}
		return offset, nil
	}
}

func (f *File) ReadFile(file *os.File, offset int64) (Key, []Message, error) {
	var node Key
	var msg []Message
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	size, err := file.ReadAt(data_node, offset)
	if size != NODE_SIZE {
		return node, msg, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {
		//读到文件末尾
		logger.DEBUG(logger.DLeader, "read All file,do not find this index")
		return node, msg, err
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	data_msg := make([]byte, node.Size)
	offset += int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	if int64(size) != node.Size {
		return node, msg, errors.New("read node size is not NODE_SIZE")
	}
	if err != nil {
		return node, msg, errors.New("read All file,do not find this index")
	}

	json.Unmarshal(data_msg, &msg)
	return node, msg, nil
}

func (f *File) ReadBytes(file *os.File, offset int64) (Key, []byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// 读取节点元数据
	data_node := make([]byte, NODE_SIZE)
	size, err := file.ReadAt(data_node, offset)

	// 检查节点元数据读取结果
	if err != nil {
		if err == io.EOF {
			logger.DEBUG(logger.DLeader, "EOF reached while reading node metadata at offset %d", offset)
			return Key{}, nil, io.EOF
		}
		logger.DEBUG(logger.DLeader, "Read error: %v", err)
		return Key{}, nil, fmt.Errorf("failed to read node metadata: %w", err)
	}
	if size != NODE_SIZE {
		logger.DEBUG(logger.DLog2, "Incomplete read: got %d bytes,expected %d\n", size, NODE_SIZE)
		return Key{}, nil, fmt.Errorf("incomplete node read: expected %d bytes, got %d", NODE_SIZE, size)
	}

	// 解析节点元数据
	var node Key
	if err := binary.Read(bytes.NewReader(data_node), binary.BigEndian, &node); err != nil {
		return Key{}, nil, fmt.Errorf("failed to decode node metadata: %w", err)
	}

	// 读取节点数据
	data_msg := make([]byte, node.Size)
	offset += int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	// 检查节点数据读取结果
	if err != nil {
		if err == io.EOF {
			logger.DEBUG(logger.DLeader, "EOF reached while reading node data at offset %d", offset+int64(NODE_SIZE))
			return node, nil, io.EOF
		}
		logger.DEBUG(logger.DLeader, "Read error: %v", err)
		return Key{}, nil, fmt.Errorf("failed to read node data: %w", err)
	}
	if int64(size) != node.Size {
		logger.DEBUG(logger.DLeader, "Incomplete read: got %d bytes,expected %d\n", size, node.Size)
		return node, nil, fmt.Errorf("incomplete node data read: expected %d bytes, got %d", node.Size, size)
	}

	return node, data_msg, nil
}
