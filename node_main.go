package main

import (
	"RAFT_KV/raft"
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func main() {
	peersIds := []int{0, 1, 2}
	readyChan := make(chan interface{})
	commitChan := make(chan raft.CommitEntry)
	serv, _ := strconv.Atoi(os.Args[1])
	var servPeersIds []int
	for i := range peersIds {
		if i == serv {
			continue
		}
		servPeersIds = append(servPeersIds, peersIds[i])
	}

	s := raft.InitServer(serv, servPeersIds, readyChan, commitChan)
	s.Serve()

	counter := 0
	for {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			text := scanner.Text()
			switch text {
			case "list":
				s.PrintMap()
				break
			case "append":
				comm := new(raft.MapCommEntry)
				comm.InitMapCommEntry("Set", s.ServerId, strconv.Itoa(counter))
				if s.Submit(comm) {
					fmt.Println("Submit success")
				} else {
					fmt.Println("Submit failed")
				}
				break
			case "s":
				readyChan <- struct{}{}
				err := s.ConnectToAllPeers()
				if err != nil {
					fmt.Println("Connect failed ", err.Error())
				}
			case "quit":
				fmt.Println("Quit")
				break
			}
		}
	}
}
