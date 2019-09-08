package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	//

	keyvalue := make(map[string]([]string) )
	for i:= 0; i< nMap;i++ {

		mapFile, err := os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			fmt.Println("Something went wrong while reading map files in doReduce")
		}

		dec := json.NewDecoder(mapFile)

		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				fmt.Println("Error while Decoding")
			}
			keyvalue[kv.Key] = append(keyvalue[kv.Key], kv.Value)
		}
		mapFile.Close()
	}
        fmt.Println("KeyValue reading complete")
		keys := make([]string, len(keyvalue))
		i := 0
		for key, _:= range keyvalue {
			keys[i] = key
			i = i + 1
		}

	    fmt.Println("KeyValue reading complete")
		sort.Strings(keys)

		outputFileName,err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err!= nil {
			fmt.Println("Something went wrong while opening out File")
		}
		enc := json.NewEncoder(outputFileName)

		fmt.Println(outFile)

		for _, key :=  range keys {
			enc.Encode(KeyValue{key, reduceF(key,  keyvalue[key])})
		}

		outputFileName.Close()


}
