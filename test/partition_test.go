package main

import (
	"DongoMQ/server"
	"fmt"
	"os"
	"testing"
)

func DeleteAllFiles(path string, t *testing.T) {
	dir, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, d := range dir {
		err = os.RemoveAll(path + d.Name())
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}

func TestPartitionAccept1(t *testing.T) {
	fmt.Println("Test: Partition accept message use addMessage")
	topicName := "phone_number"
	partName := "xian"
	fileName := "NowBlock.txt"
	messages := []string{
		"18211673055", "12345678910",
		"12345678911", "12345678912", "12345678913",
		"12345678914", "12345678915", "12345678916",
		"12345678917", "12345678918", "12345678919",
	}
	str, _ := os.Getwd()
	DeleteAllFiles(str+"/"+"Broker"+"/"+topicName+"/"+partName+"/", t)

	Partition := server.NewPartition("Broker", topicName, partName)
	path := str + "/" + "Broker" + "/" + topicName + "/" + partName + "/" + fileName
	file, fd, Err, err := server.NewFile(path)

	if err != nil {
		t.Fatal(Err, err.Error())
	}

	ret := Partition.StartGetMessage(file, fd, server.GetInfo(server.Info{
		TopicName: topicName,
		PartName:  partName,
		FileName:  fileName,
	}))
	fmt.Println("---the StartGetMessage return ", ret)

	for _, msg := range messages {
		ret, err = Partition.AddMessage(server.GetInfo(server.Info{
			Producer:  "TestPartitionAccept1",
			TopicName: topicName,
			PartName:  partName,
			Message:   []byte(msg),
			Size:      0,
			FileName:  fileName,
		}))

		if err != nil {
			t.Fatal(err.Error())
		}
	}

	_, msgs, err := file.ReadFile(fd, 0)

	for index, m := range msgs {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(" ... Passed")
}

func TestPartitionCloseAccept(t *testing.T) {
	fmt.Println("Test: Partition accept message use addMessage")

	topicName := "phone_number"
	partName := "xian"
	fileName := "NowBlock.txt"
	messages := []string{
		"18211673055", "12345678910",
		"12345678911", "12345678912", "12345678913",
		"12345678914", "12345678915", "12345678916",
		"12345678917", "12345678918", "12345678919",
	}

	str, _ := os.Getwd()
	DeleteAllFiles(str+"/"+"Broker"+"/"+topicName+"/"+partName+"/", t)

	Partition := server.NewPartition("Broker", topicName, partName)
	path := str + "/" + "Broker" + "/" + topicName + "/" + partName + "/" + fileName
	file, fd, Err, err := server.NewFile(path)

	if err != nil {
		t.Fatal(Err, err.Error())
	}

	ret := Partition.StartGetMessage(file, fd, server.GetInfo(server.Info{
		TopicName: topicName,
		PartName:  partName,
		FileName:  fileName,
	}))
	fmt.Println("---the StartGetMessage return ", ret)

	for _, msg := range messages {
		ret, err = Partition.AddMessage(server.GetInfo(server.Info{
			Producer:  "TestPartitionAccept1",
			TopicName: topicName,
			PartName:  partName,
			Message:   []byte(msg),
			Size:      0,
			FileName:  fileName,
		}))
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	start, end, ret, err := Partition.CloseAcceptMessage(server.GetInfo(server.Info{
		TopicName: topicName,
		PartName:  partName,
		FileName:  fileName,
		NewName:   "Block1.txt",
	}))

	if err != nil {
		t.Fatal("the file start", start, " end ", end, " ret ", ret, " err is ", err.Error())
	}
	fmt.Println("the file start", start, " end ", end, " ret ", ret)

	fd.Close()

	path = str + "/" + "Broker" + "/" + topicName + "/" + partName + "/" + "Block1.txt"
	file, fd, Err, err = server.NewFile(path)
	if err != nil {
		t.Fatal("the Err is ", Err, "error ", err.Error())
	}
	_, msgs, err := file.ReadFile(fd, 0)
	for index, m := range msgs {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(" ... Passed")
}
