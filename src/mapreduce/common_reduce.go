package mapreduce

import (
	"io/ioutil"
	"encoding/json"
	"sort"
	"os"
	"fmt"
)


// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()/*
	all_map := make(map[string][]string)
	//var all_array []KeyValue
	for m_idx := 0; m_idx < nMap; m_idx++ {
		file_name := reduceName(jobName, m_idx, reduceTaskNumber)
		file_content, _ := ioutil.ReadFile(file_name)
		var kv_array []KeyValue
		err := json.Unmarshal(file_content, &kv_array)
		if err != nil {
			fmt.Println(file_content)
			panic(err)
		}
		for _, kv_array_v := range kv_array {
			//all_array = append(all_array, kv_array_v)
			if _, isFound := all_map[kv_array_v.Key]; isFound == true {
				all_map[kv_array_v.Key] = append(all_map[kv_array_v.Key], kv_array_v.Value)
			} else {
				all_map[kv_array_v.Key] = []string { kv_array_v.Value }
			}
		}
	}
	var key_slice []string
	for key, _ := range all_map {
		key_slice = append(key_slice, key)
	}
	sort.Strings(key_slice)

	out_file_name := mergeName(jobName, reduceTaskNumber)
	out_file, _ := os.OpenFile(out_file_name, os.O_CREATE|os.O_WRONLY, 0)
	enc := json.NewEncoder(out_file)

	for _, sorted_key := range key_slice {
		enc.Encode(KeyValue{sorted_key, reduceF(sorted_key, all_map[sorted_key])})
	}
	out_file.Close()
}

