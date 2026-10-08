package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func randomHexString() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func WriteFile(filename string, content []byte) error {
	tempFileName, err := randomHexString()
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", tempFileName)
	if err != nil {
		return err
	}
	_, err = file.Write(content)
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), filename)
}

// intermediate files are named mr-<mapTaskId>-<bucket>, but the glob
// mr-*-<bucket> also matches our own mr-out-<bucket> and files the apps
// write (e.g. mr-worker-jobcount-<pid>-<n>), so require that the middle
// segment is a map task id.
func isIntermediateFile(filename string, bucket string) bool {
	middle := strings.TrimSuffix(strings.TrimPrefix(filename, "mr-"), "-"+bucket)
	if _, err := strconv.Atoi(middle); err != nil {
		return false
	}
	return true
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//go run mrworker.go wc.so sock123
	coordSockName = sockname
	//this will run in a loop we do not need goroutine in worker cause
	//it executes in one thread
	for {
		var workerId int

		var task Task
		ok := call("Coordinator.RequestTask", workerId, &task)
		if !ok {
			fmt.Println("something went wrong with coordinator")
			os.Exit(1)
		}

		switch task.Type {
		case TaskTypeMap:
			{
				content, err := os.ReadFile(task.FileIdentifier)
				if err != nil {
					log.Fatal(err)
				}
				KVPairs := mapf(task.FileIdentifier, string(content))
				reduceContentMap := make(map[int][]KeyValue, task.NReduce)
				for _, Pair := range KVPairs {
					bucket := ihash(Pair.Key) % task.NReduce
					_, ok := reduceContentMap[bucket]
					if !ok {
						reduceContentMap[bucket] = []KeyValue{Pair}
						continue
					}
					reduceContentMap[bucket] = append(reduceContentMap[bucket], Pair)
				}
				reportArgs := ReportArgs{
					Task: task,
				}
				for bucket, bucketContent := range reduceContentMap {
					content, err := json.Marshal(bucketContent)
					if err != nil {
						log.Fatal(err)
					}
					reduceFileName := fmt.Sprintf("mr-%d-%d", task.Id, bucket)
					//we should check if the file is already written if yes then is should not be writt
					err = WriteFile(reduceFileName, content)
					if err != nil {
						log.Fatal(err)
					}
				}

				reply := &ReportReply{}
				ok := call("Coordinator.ReportTask", reportArgs, reply)
				if !ok {
					fmt.Println("something went wrong with coordinator")
					os.Exit(1)
				}
				if !reply.Ok {
					fmt.Println("corodiantor failed to report task")
				}
			}
		case TaskTypeReduce:
			{
				partitionNumber := task.FileIdentifier
				pattern := fmt.Sprintf("mr-*-%s", partitionNumber)
				matches, err := filepath.Glob(pattern)
				if err != nil {
					log.Fatal(err)
				}
				intermediate := make([]KeyValue, 0)
				for _, filename := range matches {
					if !isIntermediateFile(filename, partitionNumber) {
						continue
					}
					file, err := os.Open(filename)
					if err != nil {
						log.Fatal(err)
					}
					fileContent, err := io.ReadAll(file)
					if err != nil {
						log.Fatal(err)
					}
					mapOutput := make([]KeyValue, 0)
					err = json.Unmarshal(fileContent, &mapOutput)
					if err != nil {
						log.Fatal(err)
					}
					intermediate = append(intermediate, mapOutput...)
					file.Close()
				}

				slices.SortFunc(intermediate, func(key1 KeyValue, key2 KeyValue) int {
					switch {
					case key1.Key < key2.Key:
						{
							return -1
						}
					case key1.Key > key2.Key:
						{
							return 1
						}
					default:
						{
							return 0
						}
					}
				})

				oname := fmt.Sprintf("mr-out-%s", partitionNumber)
				content := make([]byte, 0)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					content = append(content, []byte(fmt.Sprintf("%v %v\n", intermediate[i].Key, output))...)
					i = j
				}
				WriteFile(oname, []byte(content))
				reply := &ReportReply{}
				ok := call("Coordinator.ReportTask", ReportArgs{
					Task: task,
				}, reply)
				if !ok {
					fmt.Println("something went wrong with coordinator")
					os.Exit(1)
				}
				if !reply.Ok {
					fmt.Println("corodiantor failed to report task")
				}
			}
		case TaskTypeWait:
			{
				time.Sleep(time.Millisecond * 100)
			}
		default:
			{
				fmt.Println("recived enexpected task from coordinator exiting the worker")
				os.Exit(1)
			}
		}
	}

}

func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
