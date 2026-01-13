package main

import (
	"Node/utils"
	"flag"
	"fmt"
	"net"
	"net/rpc"
)

func main() {
	// 定义命令行参数
	identifier := flag.String("name", "Node1", "节点名称")
	port := flag.Int64("port", 8080, "节点监听的端口号")
	// recover := flag.Bool("recover", false, "是否是灾后恢复")
	// 解析命令行参数
	flag.Parse()
	// 初始化节点状态
	utils.N.Reset(*identifier, *port)
	utils.N.Init(*identifier, *port)
	utils.N.LogEntries = utils.ReadLogEntriesfromFile(*identifier, *port)
	print(len(utils.N.LogEntries))
	// 协程定时更新节点Peers
	go utils.N.UpdatePeers(*identifier, *port)

	//提前开启心跳进程，但是定时器暂停
	go utils.PeriodicalHeartbeat()

	// 注册RPC服务
	// 。。。
	rpc.Register(new(utils.VoteRQ))          // 注册Vote服务
	rpc.Register(new(utils.AppendEntriesRQ)) // 注册日志同步服务
	rpc.Register(new(utils.ReceiveEntryRQ))  //注册添加日志条目服务
	go utils.PeriodicalVoteRequest()
	go utils.PeriodicalWriteLog(*identifier, *port)
	listener, err := net.Listen("tcp", ":"+fmt.Sprint(*port))
	if err != nil {
		panic(err)
	}
	defer listener.Close() // 确保关闭监听器
	for {
		conn, err := listener.Accept() // 接受连接
		if err != nil {
			panic(err)
		}
		// 设置 TCP 连接的读写缓冲区大小（例如 1MB）
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetReadBuffer(100 * 1024 * 1024)  // 100MB 读缓冲区
			tcpConn.SetWriteBuffer(100 * 1024 * 1024) // 100MB 写缓冲区
		}
		go rpc.ServeConn(conn) // 启动一个goroutine处理连接
	}
}
