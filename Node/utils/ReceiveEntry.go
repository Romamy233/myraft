package utils

type ReceiveEntryRQArgs struct {
	Cmd string // 命令
}

type ReceiveEntryRSArgs struct {
}

type ReceiveEntryRQ struct{}

// ReceiveEntryRQHandler 处理接收日志条目请求
func (r *ReceiveEntryRQ) ReceiveEntryRQHandler(args *ReceiveEntryRQArgs, reply *ReceiveEntryRSArgs) error {
	if N.CurrentState != Leader {
		N.StateMutex.Lock()
		N.PeerMutex.Lock()
		SendRpc(N.Peers[N.CurrentLeaderId], "ReceiveEntryRQ", "ReceiveEntryRQHandler", args, reply)
		N.PeerMutex.Unlock()
		N.StateMutex.Unlock()

		return nil // 只有领导者节点处理接收日志条目请求
	}
	N.LogMutex.Lock()
	defer N.LogMutex.Unlock()
	N.StateMutex.Lock()
	defer N.StateMutex.Unlock()
	N.LogEntries = append(N.LogEntries, LogEntry{Term: N.CurrentTerm, Command: args.Cmd})
	go BoardcastAppendEntriesRQ()
	ResetHBTicker()
	return nil
}
