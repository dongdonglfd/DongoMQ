package main

import (
	"DongoMQ/server"
	"fmt"
	"strconv"
	"testing"
)

func CheckNums(PartToCon map[string][]string, min, max int, nums int) (ret string, b bool) {
	size := 0
	for k, v := range PartToCon {
		if len(v) < min {
			ret += "the consumer" + k + " responsible " + strconv.Itoa(len(v)) + " part < min(" + strconv.Itoa(min) + ")\n"
		} else if len(v) > max {
			ret += "the consumer" + k + " responsible " + strconv.Itoa(len(v)) + " part > max(" + strconv.Itoa(max) + ")\n"
		}
		size++
	}
	if size != nums {
		ret += "the consumers responsible parts quantity(" + strconv.Itoa(size) + ") != nums(" + strconv.Itoa(nums) + ")\n"
	}

	if ret != "" {
		return ret, false
	}
	return ret, true
}

func TestConsistent1(t *testing.T) {
	fmt.Println("Test: Consistent partitions > consumers")

	partitions := []string{"xian", "shanghai", "beijing"}
	consumers := []string{"consumer1", "consumer2"}

	PartToCon := make(map[string][]string)

	fmt.Println("---node is consumer")
	consistent := server.NewConsistent()

	for _, name := range consumers {
		consistent.Add(name, 1)
	}

	consistent.SetFreeNode()
	fmt.Println("---start first getnode")
	for _, name := range partitions {
		node := consistent.GetNode(name)
		PartToCon[name] = append(PartToCon[name], node)
	}

	fmt.Println("---start second getnode")
	for {
		for _, name := range partitions {
			if consistent.GetFreeNodeNum() > 0 {
				node := consistent.GetNodeFree(name)
				PartToCon[name] = append(PartToCon[name], node)
			} else {
				break
			}
		}
		if consistent.GetFreeNodeNum() <= 0 {
			break
		}
	}
	//检查partition对应的Consumer的数量是否平衡，和是否每个partition都有consumer负责
	ret, ok := CheckNums(PartToCon, 1, 2, len(partitions))
	if !ok {
		t.Fatal(ret, PartToCon)
	}
	fmt.Println(" ... Passed")
}

func TestConsistent2(t *testing.T) {
	fmt.Println("Test: Consistent partitions < consumers")

	partitions := []string{"xian", "shanghai", "beijing"}
	consumers := []string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}

	PartToCon := make(map[string][]string)

	fmt.Println("---node is consumer")
	consistent := server.NewConsistent()

	for _, name := range consumers {
		consistent.Add(name, 1)
	}
	consistent.SetFreeNode()

	fmt.Println("---start first getnode")
	for _, name := range partitions {
		node := consistent.GetNodeFree(name)
		PartToCon[name] = append(PartToCon[name], node)
	}

	fmt.Println("---start second getnode")
	for {
		for _, name := range partitions {
			if consistent.GetFreeNodeNum() > 0 {
				node := consistent.GetNodeFree(name)
				PartToCon[name] = append(PartToCon[name], node)
			} else {
				break
			}
		}
		if consistent.GetFreeNodeNum() <= 0 {
			break
		}
	}

	//检查partition对应的Consumer的数量是否平衡，和是否每个partition都有consumer负责
	ret, ok := CheckNums(PartToCon, 1, 2, len(partitions))
	if !ok {
		t.Fatal(ret, PartToCon)
	}

	fmt.Println(" ... Passed")
}

func TestConsistentBro1(t *testing.T) {
	fmt.Println("Test: ConsistentBro")

	rep_num := 3
	topicName := "phone_number"
	partitions := []string{"xian", "shanghai", "beijing"}
	brokers := []string{"Broker0", "Broker", "Broker2"}

	fmt.Println("---node is brokers")
	consistent := server.NewConsistentBro()

	for _, name := range brokers {
		consistent.Add(name, 1)
	}

	reps := make(map[string]bool)
	str := topicName + partitions[0]
	Bro_reps := consistent.GetNode(str, rep_num)
	for _, name := range Bro_reps {
		reps[name] = true
	}
	if len(reps) != rep_num {
		t.Fatal("get rep brokers ", len(reps), " != ", rep_num)
	}

	fmt.Println(" ... Passed")
}
