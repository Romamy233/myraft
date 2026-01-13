package utils

import (
	"time"

	"github.com/sirupsen/logrus"
)

type AppendEntriesRQArgs struct {
	LId          int64      // leader节点ID
	LTerm        int64      // Leader当前任期
	PrefixLength int64      // 前缀长度
	PrefixTerm   int64      // 前缀任期
	LeaderCommit int64      // Leader已提交的日志长度
	Entries      []LogEntry // 日志条目列表
}
type AppendEntriesRSArgs struct {
	FID       int64 // 节点ID
	FTerm     int64 // 当前任期
	AckLength int64 // 确认的日志长度
	Success   bool  // 是否成功接收日志
}
type AppendEntriesRQ struct{}

// AppendEntriesRQHandler 处理日志同步请求
func (r *AppendEntriesRQ) AppendEntriesRQHandler(args *AppendEntriesRQArgs, reply *AppendEntriesRSArgs) error {
	ResetVoteTicker()

	// 加锁处理状态
	N.StateMutex.Lock()
	defer N.StateMutex.Unlock()

	N.logger.Debugf("Receive AERQ from Node %d with %d entries", args.LId, len(args.Entries))

	// 更新当前节点的状态
	if args.LTerm > N.CurrentTerm {
		N.logger.Debugf("Receive AERQ from Leader %d with %d entries", args.LId, len(args.Entries))
		if N.CurrentState == Leader {
			N.HeartbeatTicker.Stop()
		}
		N.CurrentTerm = args.LTerm
		N.CurrentState = Follower
		N.VotedFor = -1
	}
	if args.LTerm == N.CurrentTerm {

		if N.CurrentState == Leader {
			N.HeartbeatTicker.Stop()
		}
		N.CurrentLeaderId = args.LId
		N.CurrentState = Follower
	}

	// 加锁处理日志
	N.LogMutex.Lock()
	defer N.LogMutex.Unlock()

	// 检查日志一致性
	logOK := (len(N.LogEntries) >= int(args.PrefixLength) &&
		(args.PrefixLength == 0 || N.LogEntries[args.PrefixLength-1].Term == args.PrefixTerm))

	if logOK && (args.LTerm == N.CurrentTerm) {
		// 追加日志条目
		AppendEntries(args.PrefixLength, args.LeaderCommit, args.Entries)
		reply.FID = N.Id
		reply.FTerm = N.CurrentTerm
		reply.AckLength = args.PrefixLength + int64(len(args.Entries))
		reply.Success = true
		go CaltoFile()
	} else {
		// 日志不一致
		reply.FID = N.Id
		reply.FTerm = N.CurrentTerm
		reply.AckLength = 0
		reply.Success = false
	}

	return nil
}

// AppendEntries 强制覆盖冲突日志条目
func AppendEntries(prefixlen int64, leadercommit int64, entries []LogEntry) {
	// 边界检查，确保 prefixlen 不超出日志长度
	if prefixlen > int64(len(N.LogEntries)) {
		N.logger.Errorf("Invalid prefix length: %d, log length: %d", prefixlen, len(N.LogEntries))
		return
	}

	// 强制覆盖冲突条目
	N.LogEntries = append(N.LogEntries[:prefixlen], entries...)

	// 更新提交长度
	if leadercommit > N.CommitLength {
		N.CommitLength = min(leadercommit, int64(len(N.LogEntries)))
	}
}

// 辅助函数：计算最小值
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// // AppendEntriesRQHandler 处理日志同步请求
// func (r *AppendEntriesRQ) AppendEntriesRQHandler(args *AppendEntriesRQArgs, reply *AppendEntriesRSArgs) error {
// 	ResetVoteTicker()
// 	// 处理日志同步请求
// 	N.StateMutex.Lock()
// 	defer N.StateMutex.Unlock()
// 	N.logger.Debugf("Receive AERQ from Node %d with %d entires",
// 		args.LId, len(args.Entries))
// 	if args.LTerm > N.CurrentTerm {
// 		if N.CurrentState == Leader {
// 			N.HeartbeatTicker.Stop()
// 		}
// 		N.CurrentTerm = args.LTerm
// 		N.CurrentState = Follower
// 		N.VotedFor = -1
// 	}
// 	if args.LTerm == N.CurrentTerm {
// 		N.CurrentLeaderId = args.LId
// 		N.CurrentState = Follower
// 	}
// 	N.LogMutex.Lock()
// 	logOK := (len(N.LogEntries) >= int(args.PrefixLength) &&
// 		(args.PrefixLength == 0 || N.LogEntries[args.PrefixLength-1].Term == args.PrefixTerm))

// 	if logOK && (args.LTerm == N.CurrentTerm) {
// 		AppendEntries(args.PrefixLength, args.LeaderCommit, args.Entries)
// 		reply.FID = N.Id
// 		reply.FTerm = N.CurrentTerm
// 		reply.AckLength = args.PrefixLength + int64(len(args.Entries))
// 		reply.Success = true
// 	} else {
// 		reply.FID = N.Id
// 		reply.FTerm = N.CurrentTerm
// 		reply.AckLength = 0
// 		reply.Success = false
// 	}
// 	N.LogMutex.Unlock()
// 	return nil
// }
// func AppendEntries(prefixlen int64, leadercommit int64, entries []LogEntry) {
// 	if len(entries) > 0 && len(N.LogEntries) >= int(prefixlen) && N.LogEntries[prefixlen-1].Term == entries[0].Term {
// 		N.LogEntries = append(N.LogEntries[:prefixlen], entries...)
// 		if leadercommit > N.CommitLength {
// 			N.CommitLength = leadercommit
// 		}
// 	}
// }

// 周期性发送心跳包
func PeriodicalHeartbeat() {
	defer N.HeartbeatTicker.Stop()
	for range N.HeartbeatTicker.C {
		N.StateMutex.Lock()
		logrus.Infof("CurrentTerm:%d beating...", N.CurrentTerm)
		N.StateMutex.Unlock()
		// N.logger.Info("Beating...")
		go BoardcastAppendEntriesRQ() // 广播心跳包
		go CaltoFile()                //把Log结果输出到文件
	}
}

func ResetHBTicker() {
	if N.HeartbeatTicker != nil {
		N.HeartbeatTicker.Stop() // 停止之前的心跳定时器
	} else {
		N.logger.Fatalf("Node %d :HeartBeatikcer failed to init", N.Id)
	}
	N.HeartbeatTicker.Reset(time.Duration(N.HeartbeatInterval) * time.Millisecond)
}

// 广播一次AppendEntriesRQ
func BoardcastAppendEntriesRQ() {
	N.PeerMutex.Lock()
	for id, port := range N.Peers {
		if id == N.Id {
			continue // 不向自己发送
		}
		go SendAppendEntriesRQ(id, port)
	}
	N.PeerMutex.Unlock()
}

func SendAppendEntriesRQ(id int64, port int64) {
	N.StateMutex.Lock()
	N.LogMutex.Lock()
	defer N.LogMutex.Unlock()
	defer N.StateMutex.Unlock()
	args := AppendEntriesRQArgs{
		LId:          N.Id,
		LTerm:        N.CurrentTerm,
		PrefixLength: N.sentLength[id],
		PrefixTerm:   N.LogEntries[N.sentLength[id]-1].Term,
		LeaderCommit: N.CommitLength,
		Entries:      N.LogEntries[N.sentLength[id]:], // 发送从PrefixLength开始的日志条目
	}

	reply := AppendEntriesRSArgs{}
	SendRpc(port, "AppendEntriesRQ", "AppendEntriesRQHandler", &args, &reply)

	if reply.FTerm > N.CurrentTerm {
		N.CurrentTerm = reply.FTerm
		N.CurrentState = Follower
		N.VotedFor = -1
		ResetVoteTicker()
	}
	if N.CurrentState == Leader && reply.FTerm == N.CurrentTerm {
		if reply.Success {
			N.sentLength[id] = reply.AckLength // 更新已发送日志长度
			N.ackLength[id] = reply.AckLength  // 更新已确认日志长度
			N.logger.Debugf("follower %d ack to sync ,len:%d", reply.FID, reply.AckLength)
			// commit日志

		} else {
			N.sentLength[id]--
			go SendAppendEntriesRQ(id, port) // 如果失败，重置发送长度
		}
	}
}
