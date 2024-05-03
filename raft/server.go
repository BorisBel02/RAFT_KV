package raft

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

type Server struct {
	mu sync.Mutex

	ServerId int
	peerIds  []int

	n *RaftNode

	rpcServer *rpc.Server
	rpcProxy  *RPCProxy
	listener  net.Listener

	commitChan  chan CommitEntry
	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup

	storage *KVStorage
}

func InitServer(ServerId int, peerIds []int, ready <-chan interface{}, commitChan chan CommitEntry) *Server {
	s := new(Server)
	s.ServerId = ServerId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan interface{})
	s.storage = InitKVStorage()
	return s
}

func (s *Server) Serve() {
	var err error
	s.mu.Lock()

	go s.storage.StartStorage(s.commitChan)

	s.n = InitRaftNode(s.ServerId, s.peerIds, s, s.ready, s.commitChan)

	s.rpcServer = rpc.NewServer()

	s.rpcProxy = &RPCProxy{n: s.n}
	err = s.rpcServer.RegisterName("RaftNode", s.rpcProxy) //register Raft service with RaftNode name
	if err != nil {
		log.Fatal("register raft service failed")
	}

	var addr net.TCPAddr
	addr.IP = net.ParseIP("127.0.0.1")
	addr.Port = 8000 + s.ServerId
	s.listener, err = net.Listen(addr.Network(), addr.String())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.ServerId, s.listener.Addr())
	
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial("tcp", addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) ConnectToAllPeers() error {
	var addr net.TCPAddr
	addr.IP = net.ParseIP("127.0.0.1")

	var err error
	for idx := range s.peerIds {
		addr.Port = 8000 + s.peerIds[idx]
		err = s.ConnectToPeer(s.peerIds[idx], &addr)
		if err != nil {
			return err
		}
		log.Println(s.ServerId, "connected to ", addr.String())
	}

	return nil
}

func (s *Server) isLeader() bool {
	_, _, ldr := s.n.ReportState()
	return ldr
}

func (s *Server) Submit(command interface{}) bool {
	return s.n.Submit(command)
}

func (s *Server) PrintMap() {
	fmt.Println(s.storage)
}

func (s *Server) SetEntry(c *gin.Context) {
	var me MapCommEntry
	if err := c.ShouldBindJSON(&me); err != nil {
		c.String(http.StatusBadRequest, err.Error())
	}
	if s.Submit(me) {
		c.JSON(http.StatusOK, me)
		return
	}
	c.JSON(http.StatusBadRequest, "Not a leader. Leader ID = "+strconv.Itoa(s.n.savedLeader))
}

func (s *Server) Die(c *gin.Context) {
	s.n.Stop()
	c.JSON(http.StatusOK, gin.H{})
}

func (s *Server) Start(c *gin.Context) {
	s.n.Start()
	c.JSON(http.StatusOK, gin.H{})
}

func (s *Server) Lock(c *gin.Context) {
	if !s.isLeader() {
		c.JSON(http.StatusInternalServerError, "Not a leader")
		return
	}
	if s.n.state == Dead {
		c.JSON(http.StatusInternalServerError, "node is dead")
		return
	}

	var savedIndex int

	for {
		s.mu.Lock()
		if !s.storage.flag {
			break
		}
		s.mu.Unlock()
	}

	savedIndex = s.n.lastApplied

	var me MapCommEntry
	me.Method = "Lock"
	s.Submit(me)
	s.mu.Unlock()

	for {
		s.mu.Lock()
		if s.n.commitIndex >= savedIndex+1 {
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
	}

	c.JSON(http.StatusOK, "LOCKED")
}

func (s *Server) Unlock(c *gin.Context) {
	if !s.isLeader() {
		c.JSON(http.StatusInternalServerError, "Not a leader")
		return
	}
	if s.n.state == Dead {
		c.JSON(http.StatusInternalServerError, "Node is dead")
		return
	}
	s.mu.Lock()
	if !s.storage.flag {
		c.JSON(http.StatusInternalServerError, "Already unlocked")
		s.mu.Unlock()
		return
	}

	var me MapCommEntry
	me.Method = "Unlock"
	s.Submit(me)
	s.mu.Unlock()

	c.JSON(http.StatusOK, "UNLOCK appended to log")
}

type RPCProxy struct {
	n *RaftNode
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	return rpp.n.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	return rpp.n.AppendEntries(args, reply)
}
