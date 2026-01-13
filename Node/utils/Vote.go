package utils

import (
	"time"
)

type VoteRQArgs struct {
	CId         int64 // 候选人ID
	CTerm       int64 // 当前任期
	LogLength   int64 // 日志长度
	LastLogTerm int64 // 最后日志的任期

}

type VoteRSArgs struct {
	NodeId      int64 // 节点ID
	Term        int64 // 当前任期
	VoteGranted bool  // 是否投票给候选人

}
type VoteRQ struct{}

// VoteRQHandler 处理投票请求
func (r *VoteRQ) VoteRQHandler(args *VoteRQArgs, reply *VoteRSArgs) error {
	// log.Default().Printf(, args.CId, args.CTerm)
	N.StateMutex.Lock()
	N.LogMutex.Lock()
	N.logger.Debugf("Curren term:%d Received VoteRQ from Node %d for term %d", N.CurrentTerm, args.CId, args.CTerm)
	ResetVoteTicker()

	defer N.StateMutex.Unlock()
	if args.CTerm > N.CurrentTerm {
		N.CurrentTerm = args.CTerm
		N.CurrentState = Follower
		N.HeartbeatTicker.Stop()

		N.VotedFor = -1
	}
	mylastLogTerm := int64(0)

	if len(N.LogEntries) > 0 {
		mylastLogTerm = N.LogEntries[len(N.LogEntries)-1].Term
	}
	logOk := (args.LastLogTerm > mylastLogTerm) ||
		(args.LastLogTerm == mylastLogTerm && args.LogLength >= int64(len(N.LogEntries)))
	N.LogMutex.Unlock()
	if args.CTerm == N.CurrentTerm && logOk && N.VotedFor == -1 {
		N.VotedFor = args.CId
		reply.NodeId = N.Id
		reply.Term = N.CurrentTerm
		reply.VoteGranted = true
		// log.Default().Printf("Node %d granted vote to Node %d for term %d", N.Id, args.CId, args.CTerm)
		N.logger.Debugf("Node %d granted vote to Node %d for term %d", N.Id, args.CId, args.CTerm)
		return nil
	}
	reply.NodeId = N.Id
	reply.Term = N.CurrentTerm
	reply.VoteGranted = false

	return nil
}

func PeriodicalVoteRequest() {
	N.EleTicker = time.NewTicker(time.Duration(N.ElectionTimeout) * time.Millisecond)
	defer N.EleTicker.Stop()
	for range N.EleTicker.C {
		StartElect() // 开始选举
	}

}
func ResetVoteTicker() {
	if N.EleTicker != nil {
		N.EleTicker.Stop() // 停止之前的定时器
	} else {
		N.logger.Fatalf("Node %d :Election ticker failed to initialize", N.Id)
	}
	N.EleTicker.Reset(time.Duration(N.ElectionTimeout) * time.Millisecond) // 重置选举定时器
}
func StartElect() {
	N.StateMutex.Lock()
	N.CurrentTerm++
	N.CurrentState = Candidate
	N.VotedFor = N.Id
	N.VoteReceived = []int64{}                    // 清空已收到的投票
	N.VoteReceived = append(N.VoteReceived, N.Id) // 自己投票给自己
	N.logger.Infof("Node %d Start vote for Term %d", N.Id, N.CurrentTerm)
	N.StateMutex.Unlock()

	go BoardcastVoteRQ()

}

// BoardcastVoteRQ 广播投票请求给所有节点
func BoardcastVoteRQ() {
	for id, port := range N.Peers {
		if id == N.Id { // 不给自己投票
			continue
		}
		go SendVoteRQ(port)
	}
}

// SendVoteRQ 发送投票请求
func SendVoteRQ(port int64) {

	N.StateMutex.Lock()
	N.LogMutex.Lock()
	var args VoteRQArgs = VoteRQArgs{
		CId:         N.Id,
		CTerm:       N.CurrentTerm,
		LogLength:   int64(len(N.LogEntries)),
		LastLogTerm: N.LogEntries[len(N.LogEntries)-1].Term,
	}
	N.StateMutex.Unlock()
	N.LogMutex.Unlock()
	var reply VoteRSArgs
	SendRpc(port, "VoteRQ", "VoteRQHandler", &args, &reply)
	if reply.VoteGranted {
		N.StateMutex.Lock()
		N.VoteReceived = append(N.VoteReceived, reply.NodeId)
		N.StateMutex.Unlock()
	}
	N.StateMutex.Lock()
	N.PeerMutex.Lock()
	defer N.PeerMutex.Unlock()
	defer N.StateMutex.Unlock()
	if N.CurrentState == Candidate && len(N.VoteReceived) > len(N.Peers)/2 {
		N.CurrentState = Leader
		N.CurrentLeaderId = N.Id
		N.VoteReceived = []int64{} // 清空已收到的投票
		for id := range N.Peers {
			N.sentLength[id] = int64(len(N.LogEntries)) // 重置发送日志长度
			N.ackLength[id] = 0                         // 重置确认日志长度
		}
		N.logger.Infof("------ %d is elected as leader in term %d------", N.Id, N.CurrentTerm)

		//成为leader
		N.EleTicker.Stop()            // 停止选举定时器
		go BoardcastAppendEntriesRQ() // 广播心跳包
		ResetHBTicker()               //重启心跳
	}

}
