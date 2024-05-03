package main

import (
	"RAFT_KV/raft"
	"bufio"
	"encoding/gob"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
)

func main() {
	peersIds := []int{0, 1, 2, 3, 4}
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
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	readyChan <- struct{}{}
	_ = s.ConnectToAllPeers()

	gob.Register(raft.MapCommEntry{})

	router := gin.Default()

	router.POST("/append_entry/", s.SetEntry)

	router.GET("/hello/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "THIS IS A RAFT SERVER")
	})

	router.GET("/example/", func(c *gin.Context) {
		var me raft.MapCommEntry
		me.Method = "Set"
		me.Args = raft.KVStruct{Key: 0, Value: "example"}

		c.JSON(http.StatusOK, me)
	})

	router.GET("/die/", s.Die)
	router.GET("/start/", s.Start)

	port := strconv.Itoa(10000 + serv)
	_ = router.Run("localhost:" + port)
}
