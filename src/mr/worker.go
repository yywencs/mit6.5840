package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample(mapf, reducef)

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := Args{}
	args.AskMessage = AskTask

	reply := Reply{}
	call("Coordinator.Handler", &args, &reply)

	for {
		deal_reply(&args, &reply, mapf, reducef)
		time.Sleep(time.Second)
		reply = Reply{}
		call("Coordinator.Handler", &args, &reply)
		// log.Println("ask message is: ", args.AskMessage)
		// log.Println("reply message is: ", reply.ReplyMessage)
		// fmt.Printf("address: %p\n", &reply)
	}

}

func deal_reply(args *Args, reply *Reply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if reply.ReplyMessage == Work {
		var err error = nil
		args.TaskInfo = reply.TaskInfo
		if reply.TaskInfo.TaskName == MapWork {
			err = map_file(reply.MapFile, mapf, reply.TaskInfo.MapNo, reply.ReduceNum)
		} else if reply.TaskInfo.TaskName == ReduceWork {
			err = reduce_file(reply.TaskInfo.ReduceNo, reducef)
		}
		if err == nil {
			args.AskMessage = Finished
		} else {
			args.AskMessage = Failed
		}
	} else if reply.ReplyMessage == Wait {
		args.AskMessage = AskTask
	} else if reply.ReplyMessage == Exit {
		os.Exit(0)
	}
}

func decode_json(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	kva := []KeyValue{}
	if err != nil {
		// log.Fatalf("cannot open json file: %v", filename)
		return kva, err
	}
	defer file.Close()
	dec := json.NewDecoder(file)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

func reduce_file(ReduceNo int, reducef func(string, []string) string) error {
	pattern := fmt.Sprintf(`^mr-\d+-%d\.json$`, ReduceNo)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	dir := "./"
	oname := "./mr-out-" + strconv.Itoa(ReduceNo)

	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	intermediate := map[string][]string{}
	// 遍历文件/子目录
	for _, file := range files {
		filename := file.Name()
		if regex.MatchString(filename) {
			file_data, err := decode_json(filepath.Join(dir, filename))
			if err != nil {
				return err
			}
			for _, kv := range file_data {
				intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
			}
		}
	}

	// ofile, err := os.Create(oname)
	tmp_file, err := ioutil.TempFile("", fmt.Sprintf("mr-reduce-tmp-%d", ReduceNo))

	if err != nil {
		return err
	}
	defer tmp_file.Close()

	for k, v := range intermediate {
		output := reducef(k, v)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp_file, "%v %v\n", k, output)
	}

	if err := os.Rename(tmp_file.Name(), oname); err != nil {
		return err
	}

	return nil
}

func read_file(filename string, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		// log.Fatalf("cannot open map file:%v", filename)
		return intermediate, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		// log.Fatalf("cannot read %v", filename)
		return intermediate, err
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	intermediate = append(intermediate, kva...)
	return intermediate, nil
}

func map_file(filename string, mapf func(string, string) []KeyValue, M int, R int) error {
	intermediate, err := read_file(filename, mapf)
	if err != nil {
		return err
	}

	partitions := make([][]KeyValue, R)

	for _, kv := range intermediate {
		y := ihash(kv.Key) % R
		partitions[y] = append(partitions[y], kv)
	}

	for i, partition := range partitions {
		if err := encode_json(partition, M, i); err != nil {
			return err
		}
	}
	return nil
}

func encode_json(intermediate []KeyValue, M int, ReduceNo int) error {
	oname := fmt.Sprintf("./mr-%d-%d.json", M, ReduceNo)

	tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", ReduceNo))
	if err != nil {
		return err
	}
	defer tempFile.Close()

	enc := json.NewEncoder(tempFile)
	for _, kv := range intermediate {
		if err := enc.Encode(&kv); err != nil {
			// log.Fatalf("can't encode kv")
			return err
		}
	}

	if err := os.Rename(tempFile.Name(), oname); err != nil {
		return err
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
