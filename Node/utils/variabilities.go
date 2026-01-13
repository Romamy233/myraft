package utils

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	Leader    = iota // 领导者状态  0
	Candidate        // 候选人状态  1
	Follower         // 跟随者状态  2
)
const (
	VoteRequest      = iota // 投票请求  0
	LogRequest              // 日志请求  1
	HeartbeatRequest        // 心跳请求  2
)

type LogEntry struct {
	Term    int64  // 日志条目的任期
	Command string // 日志条目的命令
}

// Raft系统用
type RaftNode struct {
	Id              int64           // 节点ID
	CurrentState    int             // 当前状态
	CurrentTerm     int64           // 当前任期
	LogEntries      []LogEntry      // 日志条目列表
	VotedFor        int64           // 投票给的候选人ID
	CommitLength    int64           // 已提交的日志长度
	CurrentLeaderId int64           // 当前领导者ID
	Peers           map[int64]int64 // 其他节点的ID、Port

	VoteReceived []int64 // 收到投票的ID

	sentLength map[int64]int64 // 发送日志长度，key: 节点ID, value: 日志长度
	ackLength  map[int64]int64 // 确认日志长度，key: 节点ID, value: 日志长度

	HeartbeatInterval int64        // 心跳间隔时间，单位为毫秒
	HeartbeatTicker   *time.Ticker // 心跳定时器

	ElectionTimeout int64        // 超时选举时间，单位为毫秒
	EleTicker       *time.Ticker // 选举定时器

	Port int64 // 节点监听的端口号

	PeerMutex  sync.Mutex // 互斥锁，用于保护节点状态的并发访问
	LogMutex   sync.Mutex // 互斥锁，用于保护日志条目的并发访问
	StateMutex sync.Mutex // 互斥锁，用于保护投票状态的并发访问
	logger     *logrus.Logger
}

// 和Core通信用
type Node struct {
	ID   int64 // 节点ID
	Port int64 // 节点监听的端口号}
}

// InitRQArgs 用于RPC请求的参数
type InitRQArgs struct {
	Identifier string // 节点标识
	Port       int64  // 节点监听的端口号
}
type InitRSArgs struct {
	ID    int64  // 节点ID
	Peers []Node // 其他节点的ID列表
}

// 创建节点实例
var N RaftNode = RaftNode{}

func SendRpc(port int64, service string, method string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		// N.logger.Warnf()
		logrus.Warnf("Failed to connect to Node at port %d: %v", port, err)
		return err
	}
	defer client.Close()
	err = client.Call(service+"."+method, args, reply)
	if err != nil {
		// N.logger.Warnf("Failed to call %s.%s on Node at port %d: %v", service, method, port, err)
		logrus.Warnf("Failed to call %s.%s on Node at port %d: %v", service, method, port, err)
		return err
	}
	logrus.Tracef("Successfully called %s.%s on Node at port %d", service, method, port)
	return nil
}

func PeriodicalWriteLog(name string, port int64) {
	for {
		time.Sleep(20 * time.Second)
		WriteLogEntiestoFile(name, port)
	}
}

func WriteLogEntiestoFile(name string, port int64) {
	file_name := fmt.Sprintf("LogEntries.Node%d", N.Id)
	fileid, err := os.OpenFile(file_name, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("打开文件失败: %v\n", err)
		return
	}
	defer fileid.Close()
	// utils.N.StateMutex.Lock()
	// prefixcontent := []byte(fmt.Sprintf("---------CurrentTerm:%d  Time:%s-------\n",
	// 	utils.N.CurrentTerm, time.Now().Format("15:04:05")))
	// utils.N.StateMutex.Unlock()
	// fileid.Write(prefixcontent)
	for _, entry := range N.LogEntries {
		WriteOneEntrytoFile(fileid, entry)
	}
	fileid.Write([]byte("\n"))
}

func WriteOneEntrytoFile(fileid *os.File, entry LogEntry) {
	content := []byte(fmt.Sprintf("Term:%d Cmd:%s\n", entry.Term, entry.Command))
	fileid.Write(content)
}

// 从log文件恢复
func ReadLogEntriesfromFile(name string, port int64) []LogEntry {
	file_name := fmt.Sprintf("LogEntries.Node%d", N.Id)
	file, err := os.Open(file_name)
	if err != nil {
		N.logger.Warn("Recover:open log file fail, creating new file")
		file, err = os.Create(file_name)
		if err != nil {
			N.logger.Error("Recover:create log file fail")
			return []LogEntry{{Term: 0, Command: "+0"}}
		}
		defer file.Close()
		return []LogEntry{{Term: 0, Command: "+0"}}
	}
	defer file.Close()

	var logEntries []LogEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Term:") {
			var term int64
			var command string
			_, err := fmt.Sscanf(line, "Term:%d Cmd:%s", &term, &command)
			if err == nil {
				logEntries = append(logEntries, LogEntry{Term: term, Command: command})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("读取文件时发生错误: %v\n", err)
	}

	return logEntries
}

func CaltoFile() {
	N.LogMutex.Lock()
	tmp := CalLogentries{LEs: N.LogEntries}
	N.LogMutex.Unlock()
	timebuf := time.Now().Format("15:03:04")
	content := []byte(fmt.Sprintf("%s:%d\n", timebuf, tmp.cal(0)))
	file_name := fmt.Sprintf("Outcome.node%d", N.Id)
	file, err := os.OpenFile(file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		N.logger.Warn("Try open outcome file fail")
		return
	}
	defer file.Close()
	_, err = file.Write(content)
	if err != nil {
		N.logger.Warn("Try write content fail")
	}
	N.logger.Trace("Write outcome success")
}

type CalLogentries struct {
	LEs []LogEntry
}

func (cle CalLogentries) cal(src int) int {
	var ret int = src
	for _, entry := range cle.LEs {
		operator := entry.Command[0]
		num := int(entry.Command[1] - '0')
		switch operator {
		case '+':
			ret += num
		case '-':
			ret -= num
		case '*':
			ret *= num
		}
	}
	return ret
}
