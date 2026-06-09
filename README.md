# myraft

基于 Go 原生 `net/rpc` 库实现的 Raft 共识协议 Demo，包含 Leader 选举、日志复制（AppendEntries）、心跳保活等核心机制。

## 项目结构

```
myraft/
├── admin/                         # Admin 模块：服务注册 & 命令注入
│   ├── SD.go                      # 入口：启动 RPC 注册服务 + 随机发送命令
│   ├── go.mod / go.sum
│   └── utils/                     # 与 Node 共享的 Raft 核心代码
│       ├── variabilities.go       # 数据结构定义 + RPC 工具 + 日志持久化 + 结果计算
│       ├── Init.go                # 节点初始化和 Peers 同步
│       ├── Vote.go                # Leader 选举（VoteRequest RPC）
│       ├── LogSync.go             # 日志复制（AppendEntries RPC）+ 心跳
│       └── ReceiveEntry.go        # 接收客户端命令 RPC
├── Node/                          # Node 模块：Raft 节点
│   ├── Node.go                    # 入口：启动 Raft 节点，注册 RPC 服务
│   ├── start.sh                   # 批量启动脚本（一键启动 N 个节点）
│   ├── Makefile                   # clean 命令
│   ├── go.mod / go.sum
│   ├── Log/                       # 日志输出目录（JSON 格式）
│   └── utils/                     # Raft 核心实现（同上）
│       ├── variabilities.go
│       ├── Init.go
│       ├── Vote.go
│       ├── LogSync.go
│       └── ReceiveEntry.go
├── LogEntries.Node*               # 各节点持久化的日志条目（运行中产生）
├── Outcome.node*                  # 各节点的状态机计算结果（运行中产生）
└── log.node*.json                 # 各节点运行日志（运行中产生）
```

## 架构设计

该项目由两个独立模块组成：

| 角色 | 端口 | 职责 |
|------|------|------|
| **Admin** | `20000` | 服务注册中心，节点启动后向 Admin 注册获取 ID 和集群 Peer 列表；同时负责向集群随机注入算术命令 |
| **Node** | 自定义（如 `8081`~`8090`） | Raft 节点，参与 Leader 选举、日志复制、心跳保活，共同维护一个复制状态机 |

### RPC 服务

| RPC 服务 | 用途 |
|----------|------|
| `InitRQ.InitRQHandler` | Node 向 Admin 注册，获取 ID 和 Peers 列表 |
| `VoteRQ.VoteRQHandler` | 候选者向其他节点请求投票 |
| `AppendEntriesRQ.AppendEntriesRQHandler` | Leader 向 Follower 同步日志 + 心跳 |
| `ReceiveEntryRQ.ReceiveEntryRQHandler` | 接收新命令（非 Leader 自动转发给 Leader） |

### Raft 核心流程

1. **启动 & 注册**：Node 启动后调用 Admin 的 `InitRQ` 获取集群信息，之后每 5 秒刷新一次 Peers
2. **Leader 选举**：每个节点有随机的选举超时（15~25 秒），超时后发起 `VoteRequest` 广播，获得多数票后成为 Leader
3. **日志复制**：Leader 通过 `AppendEntries` 将日志条目复制到所有 Follower，冲突时强制覆盖
4. **心跳保活**：Leader 每 5 秒发送一轮心跳（空 `AppendEntries`）
5. **命令注入**：Admin 每隔 100ms 随机选一个节点发送 `+N` 或 `-N` 命令

### 状态机

本项目的"状态机"是一个简单的累加器（从 0 开始）：

- 日志命令格式：`+3`、`-2`、`*5`（运算符 + 个位数数字）
- 所有节点独立计算，结果写入 `Outcome.node{N}` 文件
- 一致时，所有节点的最终结果应该相同

## 快速开始

### 环境要求

- Go 1.23+
- Linux / macOS（`start.sh` 为 bash 脚本；Windows 需手动逐个启动）

### 编译

```bash
# 编译 Admin
cd admin && go build -o admin SD.go

# 编译 Node
cd Node && go build -o Node Node.go
```

### 运行

**第一步：启动 Admin**

```bash
cd admin
./admin -times 1200    # -times 指定发送的命令数量，默认 1200
```

**第二步：启动 Node 节点**

方式一 — 使用脚本批量启动（推荐）：

```bash
cd Node
# 修改 start.sh 中 START_PORT / END_PORT 控制节点数量和端口范围
bash start.sh
```

默认启动 10 个节点（端口 8081~8090，名称 node1~node10）。

方式二 — 手动逐个启动：

```bash
cd Node
./Node -name node1 -port 8081 &
./Node -name node2 -port 8082 &
./Node -name node3 -port 8083 &
# ...
```

**第三步：注入命令**

Admin 启动后会在终端提示"按回车键开始发送命令"，按下回车后自动向集群注入命令。

### 参数说明

| 程序 | 参数 | 默认值 | 说明 |
|------|------|--------|------|
| Admin | `-times` | `1200` | 发送的命令总条数 |
| Node | `-name` | `Node1` | 节点名称（用于向 Admin 注册） |
| Node | `-port` | `8080` | 节点监听端口 |

### 清理

```bash
# 停止所有 Node 进程
pkill -f Node

# 清理运行产生的文件
cd Node && make clean
```

## 输出文件说明

| 文件 | 说明 |
|------|------|
| `LogEntries.Node{N}` | 节点持久化的日志条目（Term + Command），启动时自动恢复 |
| `Outcome.node{N}` | 节点的状态机计算结果（时间戳 + 累加值） |
| `Log/log.node{N}.json` | 节点运行日志（JSON 格式，Logrus 输出） |

## 技术要点

- 完全基于 Go 标准库 `net/rpc`，无需第三方 RPC 框架
- 使用 `sync.Mutex` 保护并发读写（StateMutex / LogMutex / PeerMutex）
- 使用 `time.Ticker` 实现周期性心跳和选举超时
- 日志持久化到本地文件，重启后可恢复
- 每次 `AppendEntries` 成功后触发状态机计算（`CaltoFile`）
- 支持日志强制覆盖解决冲突（日志冲突时 Follower 的后缀直接被 Leader 的日志覆盖）

## RaftNode 核心状态变量

| 变量 | 类型 | 说明 |
|------|------|------|
| `Id` | `int64` | 节点 ID（由 Admin 分配） |
| `CurrentState` | `int` | 当前角色：0=Leader, 1=Candidate, 2=Follower |
| `CurrentTerm` | `int64` | 当前任期号 |
| `LogEntries` | `[]LogEntry` | 日志条目列表 |
| `VotedFor` | `int64` | 当前任期投给谁（-1 表示未投票） |
| `CommitLength` | `int64` | 已提交的日志长度 |
| `CurrentLeaderId` | `int64` | 当前已知的 Leader ID |
| `Peers` | `map[int64]int64` | 其他节点：ID -> Port |
| `sentLength` | `map[int64]int64` | 对每个节点已发送的日志长度 |
| `ackLength` | `map[int64]int64` | 对每个节点已确认的日志长度 |
| `HeartbeatInterval` | `int64` | 心跳间隔（5000ms） |
| `ElectionTimeout` | `int64` | 选举超时（15000~25000ms 随机） |
