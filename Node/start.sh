#!/bin/bash

# ===================== 配置参数（可根据需要修改）=====================
# 起始端口号
START_PORT=8081
# 结束端口号（决定运行多少个节点，比如8085则运行5个节点）
END_PORT=8090
# 起始节点编号（对应node1中的1）
START_NODE_NUM=1
# Node程序路径（确保路径正确）
NODE_PROGRAM="./Node"
# ====================================================================

# 检查Node程序是否存在且可执行
if [ ! -x "$NODE_PROGRAM" ]; then
    echo "错误：$NODE_PROGRAM 不存在或没有执行权限！"
    echo "请检查程序路径是否正确，或执行 chmod +x $NODE_PROGRAM 赋予权限"
    exit 1
fi

# 计算要运行的节点数量
NODE_COUNT=$((END_PORT - START_PORT + 1))
echo "即将批量运行 $NODE_COUNT 个Node节点："
echo "起始端口：$START_PORT，结束端口：$END_PORT"
echo "起始节点编号：$START_NODE_NUM"
echo "========================================"

# 循环运行节点程序
current_port=$START_PORT
current_node_num=$START_NODE_NUM

while [ $current_port -le $END_PORT ]; do
    # 拼接完整命令
    run_cmd="$NODE_PROGRAM -name node$current_node_num -port $current_port"
    
    echo "正在运行：$run_cmd"
    # 后台运行程序（&符号），如果需要前台运行可去掉&
    $run_cmd &
    
    # 记录进程ID（可选，方便后续管理）
    PID=$!
    echo "节点 node$current_node_num (端口 $current_port) 已启动，PID: $PID"
    
    # 数字递增
    current_port=$((current_port + 1))
    current_node_num=$((current_node_num + 1))
    
    # 可选：每次启动后延迟1秒，避免瞬间启动过多进程（可根据需要调整）
    sleep 1
done

echo "========================================"
echo "所有节点已启动！"
echo "查看运行中的Node进程：ps -ef | grep Node"
echo "停止所有Node进程：pkill -f Node"