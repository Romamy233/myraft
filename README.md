# myraft
## 项目说明
### 简介
本项目基于go原生rpc库实现raft系统核心调用功能（AppendEntry、VoteRequest）与心跳保活，使用Logrus库分级输出日志。
### 启动流程
admin作为服务注册节点启动，Node节点作为raft独立节点先向admin注册，之后持续向admin发起更新系统节点信息Rpc
- ./example -h 可查看进程使用方式 