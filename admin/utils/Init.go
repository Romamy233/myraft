package utils

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// "os"
func (n *RaftNode) Reset(name string, port int64) {
	n.Id = -1
	n.CurrentState = Follower
	n.CurrentTerm = 0

	n.VotedFor = -1
	n.CommitLength = 0
	n.CurrentLeaderId = -1
	n.Peers = make(map[int64]int64)

	n.VoteReceived = []int64{}

	n.sentLength = make(map[int64]int64)
	n.ackLength = make(map[int64]int64)

	n.HeartbeatInterval = 5000
	n.ElectionTimeout = 15000 + int64(rand.Intn(10000)) // 选举超时时间为15秒到25秒之间的随机值

	n.HeartbeatTicker = time.NewTicker(time.Duration(N.HeartbeatInterval) * time.Millisecond)
	n.HeartbeatTicker.Stop()

	n.Port = port

	n.logger = logrus.New()
	n.logger.SetLevel(logrus.DebugLevel)
	n.logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "15:04:05.000", // 带毫秒的时间格式
	})
	file, err := os.OpenFile(fmt.Sprintf("log.%s.json", name),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		n.logger.Fatalf("无法打开文件: %v\n", err)
		return
	}
	n.logger.SetOutput(file)

	// n.LogEntries = []LogEntry{{Term: 0, Command: "+0"}}
}

func (n *RaftNode) Init(identifier string, port int64) {
	// RPC
	var args InitRQArgs = InitRQArgs{
		Identifier: identifier,
		Port:       port,
	}
	var reply InitRSArgs

	SendRpc(20000, "InitRQ", "InitRQHandler", &args, &reply)

	n.Id = reply.ID
	n.Port = args.Port
	n.PeerMutex.Lock()
	N.LogMutex.Lock()
	for _, peer := range reply.Peers {
		_, exists := n.Peers[peer.ID]
		if !exists {
			n.sentLength[peer.ID] = int64(len(n.LogEntries))
		}
		n.Peers[peer.ID] = peer.Port
	}
	n.LogMutex.Unlock()
	n.PeerMutex.Unlock()
	//打印Peers
	// for id, port := range n.Peers {
	// 	log.Default().Printf("Peer ID: %d, Port: %d\n", id, port)
	// }
}
func (n *RaftNode) UpdatePeers(identifier string, port int64) {
	for {
		time.Sleep(5 * time.Second) // 每5秒更新一次
		n.Init(identifier, port)
	}

}
