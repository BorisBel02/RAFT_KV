package raft

import "fmt"

type KvStorage struct {
	storage map[int]string
}

func initStorage() *KvStorage {
	s := new(KvStorage)
	s.storage = make(map[int]string)

	return s
}

func (s *KvStorage) Append(key int, value string) {
	s.storage[key] = value
}
func (s *KvStorage) Delete(key int) {
	delete(s.storage, key)
}
func (s *KvStorage) print() {
	for key, val := range s.storage {
		fmt.Println("Key: ", key, ", Value ", val)
	}
}
