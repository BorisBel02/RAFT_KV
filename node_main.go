package main

import (
	"RAFT_KV/raft"
	"encoding/gob"
	"github.com/gin-gonic/gin"
	"net/http"
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

	//counter := 0
	readyChan <- struct{}{}
	_ = s.ConnectToAllPeers()

	gob.Register(raft.MapCommEntry{})

	router := gin.Default()
	router.POST("/map/", s.SetEntry)
	router.GET("/hello", func(c *gin.Context) {
		c.JSON(http.StatusOK, "THIS IS A RAFT SERVER")
	})
	router.Run("localhost:6000")
}
