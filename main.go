package main

import (
	"RAFT_KV/raft"
	"encoding/gob"
	"fmt"
	"time"
)

func main() {
	peersIds := []int{1, 2, 3}
	readyChan := make(chan interface{})
	commitChan := make(chan raft.CommitEntry)

	go func(commitChan chan raft.CommitEntry) {
		for {
			val, ok := <-commitChan
			if ok {
				fmt.Println(val, ok)
			} else {
				fmt.Println(val, ok, "loop broke")
				break
			}
		}
	}(commitChan)

	servers := make(map[int]*raft.Server)
	//serv is index of server in peersIds
	for serv := range peersIds {
		var servPeersIds []int
		for i := range peersIds {
			if i == serv {
				continue
			}
			servPeersIds = append(servPeersIds, peersIds[i])
		}

		s := raft.InitServer(peersIds[serv], servPeersIds, readyChan, commitChan)
		s.Serve()
		servers[serv] = s
	}

	for serv := range peersIds {
		err := servers[serv].ConnectToAllPeers()
		if err != nil {
			fmt.Println("Connect failed ", err.Error())
		}
	}

	readyChan <- struct{}{}
	counter := 0
	time.Sleep(6 * time.Second)
	gob.Register(raft.MapCommEntry{})
	for {
		me := raft.MapCommEntry{Method: "Set", Args: struct {
			Key   int
			Value string
		}{Key: counter, Value: "aboba"}}
		counter++
		time.Sleep(5 * time.Second)
		var server *raft.Server
		for id := range servers {
			server = servers[id]
			if server.Submit(me) {
				break
			}
		}
	}
}
