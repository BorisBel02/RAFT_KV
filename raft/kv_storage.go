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
		log.Println("new entry to commit")
		if !ok {
			fmt.Println("commit chan failure in storage")

		}
		if mapEntry, ok := comm.Command.(MapCommEntry); !ok {
			fmt.Println("command is not a MapCommEntry")
			return
		} else {
			method := reflect.ValueOf(kvStorage).MethodByName(mapEntry.method)
			if method.IsValid() {
				method.Call([]reflect.Value{reflect.ValueOf(mapEntry.args)})
			}
		}
	}
}

func (kvStorage *KVStorage) Set(args KVStruct) {
	kvStorage.storage[args.key] = args.value
}

func (kvStorage *KVStorage) Get(args KVStruct) string {
	value := kvStorage.storage[args.key]
	fmt.Println(value)
	return value
}

func (kvStorage *KVStorage) Delete(args KVStruct) {
	delete(kvStorage.storage, args.key)
}

func (kvStorage *KVStorage) Clear(args KVStruct) {
	kvStorage.storage = make(map[int]string)
}

func (kvStorage *KVStorage) Size(args KVStruct) int {
	return len(kvStorage.storage)
}

type KVStruct struct {
	key   int
	value string
}
type MapCommEntry struct {
	method string
	args   KVStruct
}

func (mce *MapCommEntry) InitMapCommEntry(method string, key int, value string) {
	mce.method = method
	mce.args = KVStruct{key: key, value: value}
}
