package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
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
	// file.Close()
	//
	kvs := make(map[string][]string)
	ch := make(chan []KeyValue, nMap)
	for mapTaskNumber := 0; mapTaskNumber < nMap; mapTaskNumber++ {
		intervalFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		go func(intervalFileName string) {
			var intervalKVs []KeyValue
			intervalFile, err := os.Open(intervalFileName)
			if err != nil {
				ch <- intervalKVs
				return
			}
			dec := json.NewDecoder(intervalFile)
			for {
				var kv KeyValue
				err = dec.Decode(&kv)
				if err != nil {
					break
				}
				intervalKVs = append(intervalKVs, kv)
			}
			intervalFile.Close()
			ch <- intervalKVs
		}(intervalFileName)
	}
	//TODO concurrentHashMap
	for i := 0; i < nMap; i++ {
		internalkvs := <-ch
		for _, kv := range internalkvs {
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	close(ch)
	//var keys []string
	//for k := range kvs {
	//	keys = append(keys, k)
	//}
	//sort.Strings(keys)
	//writer, err := os.Create(outFile)
	//if err != nil {
	//	return
	//}
	//enc := json.NewEncoder(writer)
	//for _, key := range keys {
	//	enc.Encode(KeyValue{key, reduceF(key, kvs[key])})
	//}
	//writer.Close()

	//是否才用异步视 reduceF的耗时定
	var reduceResultMap = make(map[string]*KeyValue)
	ch1 := make(chan *KeyValue, len(kvs))
	for key, value := range kvs {
		go func(key string, value []string) {
			ch1 <- &KeyValue{key, reduceF(key, value)}
		}(key, value)
	}
	for i := 0; i < len(kvs); i++ {
		keyPair := <-ch1
		reduceResultMap[keyPair.Key] = keyPair
	}
	close(ch1)
	var keys []string
	for k := range reduceResultMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	writer, err := os.Create(outFile)
	if err != nil {
		return
	}
	enc := json.NewEncoder(writer)
	for _, key := range keys {
		enc.Encode(reduceResultMap[key])
	}
	writer.Close()

}
