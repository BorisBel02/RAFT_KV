package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu sync.Mutex

	ServerId int
	peerIds  []int

	n *RaftNode

	rpcServer *rpc.Server
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
	s.mu.Lock()
	go s.storage.StartStorage(s.commitChan)
	s.n = InitRaftNode(s.ServerId, s.peerIds, s, s.ready, s.commitChan)
	s.rpcServer = rpc.NewServer()
	_ = s.rpcServer.Register(s.n) //register Raft service with RaftNode name

	var err error
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
		client, err := rpc.Dial(addr.Network(), addr.String())
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
