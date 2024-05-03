package raft

import (
	"fmt"
	"log"
	"reflect"
)

type KVStorage struct {
	storage map[int]string
}

func InitKVStorage() *KVStorage {
	kvs := new(KVStorage)
	kvs.storage = make(map[int]string)
	return kvs
}

func (kvStorage *KVStorage) StartStorage(commitChan chan CommitEntry) {
	log.Println("storage started")
	for {
		comm, ok := <-commitChan
		log.Printf("new entry to commit %+v\n", comm)
		if !ok {
			log.Println("commit chan failure in storage")

		}
		if mapEntry, ok := comm.Command.(MapCommEntry); !ok {
			log.Fatalf("command: %s is not a MapCommEntry", comm.Command)
			return
		} else {
			method := reflect.ValueOf(kvStorage).MethodByName(mapEntry.Method)
			if method.IsValid() {
				method.Call([]reflect.Value{reflect.ValueOf(mapEntry.Args)})
			} else {
				log.Fatalf("method %s %+v invalid\n", comm.Command, mapEntry.Args)
			}
		}
	}
}

func (kvStorage *KVStorage) Set(args KVStruct) {
	kvStorage.storage[args.Key] = args.Value
	log.Println(kvStorage.storage)
}

func (kvStorage *KVStorage) Get(args KVStruct) string {
	value := kvStorage.storage[args.Key]
	fmt.Println(value)
	return value
}

func (kvStorage *KVStorage) Delete(args KVStruct) {
	delete(kvStorage.storage, args.Key)
}

func (kvStorage *KVStorage) Clear(args KVStruct) {
	kvStorage.storage = make(map[int]string)
}

func (kvStorage *KVStorage) Size(args KVStruct) int {
	return len(kvStorage.storage)
}

type KVStruct struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}
type MapCommEntry struct {
	Method string   `json:"method"`
	Args   KVStruct `json:"args"`
}

func (mce *MapCommEntry) InitMapCommEntry(method string, Key int, value string) {
	mce.Method = method
	mce.Args = KVStruct{Key: Key, Value: value}
}
