package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

const (
	SleepDuration = 100 * time.Millisecond
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Client struct {
	address string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	client := Client{
		address: sockname,
		mapf:    mapf,
		reducef: reducef,
	}

	client.Run()
}

func (c *Client) Run() {
	for {
		ok, response := c.GetJob()
		if !ok {
			break
		}

		switch response.TaskType {
		case MapTask:
			result := c.runMapFunction(response)
			c.sendResult(result)
		case ReduceTask:
			result := c.runReduceFunction(response)
			c.sendResult(result)
		case SleepTask:
			time.Sleep(SleepDuration)
		}
	}
}

func (c *Client) GetJob() (bool, GetJobResponse) {
	args := GetJobRequest{}
	reply := GetJobResponse{}

	ok := c.call("Coordinator.GetJob", &args, &reply)
	if ok {
		return true, reply
	} else {
		fmt.Printf("call failed!\n")
		return false, reply
	}
}

func (c *Client) sendResult(result FinishJobRequest) {
	c.call("Coordinator.OnJobFinish", &result, &FinishJobResponse{})
}

func (c *Client) runMapFunction(response GetJobResponse) FinishJobRequest {
	content, exists := readFile(response.Input)
	if exists == false {
		log.Fatal("Error: file doesn't exists")
	}
	kva := c.mapf(response.Input, string(content))

	fileToPairs := make(map[int][]KeyValue, response.Buckets)
	for _, value := range kva {
		bucket := ihash(value.Key) % response.Buckets
		fileToPairs[bucket] = append(fileToPairs[bucket], value)
	}

	for bucket, pairs := range fileToPairs {
		tempFile, err := os.CreateTemp("./", "")
		if err != nil {
			log.Fatal("Error:", err)
		}
		for _, pair := range pairs {
			fmt.Fprintf(tempFile, "%v %v\n", pair.Key, pair.Value)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", response.TaskNumber, bucket))
	}

	return FinishJobRequest{
		TaskType:   response.TaskType,
		TaskNumber: response.TaskNumber,
	}
}

func (c *Client) runReduceFunction(response GetJobResponse) FinishJobRequest {
	kva := make(map[string][]string, 0)

	for index := range response.Maps {
		content, exists := readFile(fmt.Sprintf("mr-%d-%d", index, response.TaskNumber))
		if exists == false {
			continue
		}
		pairs := strings.Split(string(content), "\n")
		for _, pair := range pairs {
			if pair == "" {
				continue
			}
			parts := strings.SplitN(pair, " ", 2)
			key, value := parts[0], parts[1]
			if kva[key] == nil {
				kva[key] = []string{}
			}
			kva[key] = append(kva[key], value)
		}
	}

	tempFile, err := os.CreateTemp("./", "")
	if err != nil {
		log.Fatal("Error:", err)
	}

	for key, values := range kva {
		output := c.reducef(key, values)
		fmt.Fprintf(tempFile, "%v %v\n", key, output)
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", response.TaskNumber))

	return FinishJobRequest{
		TaskType:   response.TaskType,
		TaskNumber: response.TaskNumber,
	}
}

func readFile(filename string) ([]byte, bool) {
	file, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content, true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (c *Client) call(rpcname string, args interface{}, reply interface{}) bool {
	// Windows-native port: TCP loopback transport (coordSockName is a
	// "127.0.0.1:port" address) instead of a Unix-domain socket.
	rpcClient, err := rpc.DialHTTP("tcp", c.address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer rpcClient.Close()

	if err := rpcClient.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
