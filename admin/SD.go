// 服务发现
package main

import (
	"admin/utils"
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	Nodes = make(map[string]utils.Node) // 节点列表，key为节点标识，value为节点Node结构体
)

//RPC初始化，节点获取ID以及集群其余节点的ID

type InitRQ struct{}

var MapMutex sync.Mutex

// InitRQHandler 处理初始化请求
func (r *InitRQ) InitRQHandler(args *utils.InitRQArgs, reply *utils.InitRSArgs) error {
	// 检查节点是否已存在
	MapMutex.Lock()
	defer MapMutex.Unlock()
	if _, exists := Nodes[args.Identifier]; exists {
		// 更新端口
		node := Nodes[args.Identifier] // 取出副本
		node.Port = args.Port          // 修改字段
		Nodes[args.Identifier] = node  // 放回 map
		// 准备回复参数
		reply.ID = Nodes[args.Identifier].ID
		reply.Peers = make([]utils.Node, 0, len(Nodes))
		for _, node := range Nodes {
			reply.Peers = append(reply.Peers, node)
		}
		// log.Printf("Node %s already exists with ID %d and port %d",
		// args.Identifier, Nodes[args.Identifier].ID, Nodes[args.Identifier].Port)
		return nil
	}
	// 创建新节点并添加到节点列表
	newNode := utils.Node{
		ID:   int64(len(Nodes) + 1), // 简单的ID分配方式
		Port: args.Port,
	}
	Nodes[args.Identifier] = newNode
	// 准备回复参数
	reply.ID = newNode.ID
	reply.Peers = make([]utils.Node, 0, len(Nodes))
	for _, node := range Nodes {
		reply.Peers = append(reply.Peers, node)
	}
	log.Printf("Node %s initialized with ID %d and port %d", args.Identifier, newNode.ID, newNode.Port)
	return nil
}
func main() {
	times := flag.Int("times", 1200, "命令个数")
	flag.Parse()
	// 启动RPC服务器
	rpc.Register(new(InitRQ))                    // 注册RPC服务
	listener, err := net.Listen("tcp", ":20000") // 监听端口
	if err != nil {
		panic(err)
	}
	defer listener.Close() // 确保关闭监听器
	go SendCmd(*times)
	for {
		conn, err := listener.Accept() // 接受连接
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn) // 启动一个goroutine处理连接
	}
}

func SendCmd(times int) {
	// 等待用户输入
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("按回车键开始发送命令...")
	_, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("读取输入时出错: %v", err)
		return
	}

	fmt.Println("开始发送命令...")
	MapMutex.Lock()
	length := len(Nodes)
	MapMutex.Unlock()

	Operators := []string{"+", "-"} // 使用 [] 初始化切片
	for i := 0; i < times; i++ {
		time.Sleep(100 * time.Millisecond)
		if length == 0 {
			continue
		}
		tar := rand.Intn(length)
		MapMutex.Lock()
		for _, node := range Nodes {
			if node.ID == int64(tar) {
				utils.SendRpc(node.Port, "ReceiveEntryRQ", "ReceiveEntryRQHandler",
					&utils.ReceiveEntryRQArgs{Cmd: fmt.Sprintf("%s%d", Operators[rand.Intn(len(Operators))], rand.Intn(9)+1)},
					&utils.ReceiveEntryRSArgs{})
			}
		}
		MapMutex.Unlock()
	}
	log.Print("Msg send done")
}
