# DongoMQ

DongoMQ 是一款使用 Go 语言开发的分布式消息队列中间件，面向 Linux 环境设计。它通过 ZooKeeper 管理元数据，通过 Kitex 暴露 RPC 接口，并支持消息持久化、订阅消费和多副本同步。

## 功能特性

- 支持 Topic 和 Partition 模型，Topic 可拆分为多个 Partition 提升吞吐能力。
- 支持 PTP 和 PSB 两种消费模式，适配点对点消费和发布订阅消费场景。
- 支持 Push 和 Pull 两种消费方式，消费者可以主动拉取，也可以由 Broker 主动推送。
- 支持多副本同步，Producer 可通过 ack 参数选择不同的一致性策略。
- 支持基于 Raft 的副本同步，也支持 Leader 写入后 Follower 拉取的 fetch 同步方式。
- 使用 ZooKeeper 存储 Broker、Topic、Partition、Block、Replica 和订阅关系等元数据。
- 消息顺序写入磁盘，并通过块索引记录消息范围，降低随机 IO 成本。

## 架构概览

DongoMQ 主要由以下角色组成：

- Producer：创建 Topic/Partition，设置 Partition 同步状态，并向指定 Partition 写入消息。
- Consumer：订阅 Topic 或 Partition，通过 Push 或 Pull 模式消费消息。
- Broker：负责接收、持久化、复制和分发消息。
- ZKServer：DongoMQ 内部的元数据协调服务，负责连接 ZooKeeper 并调度 Broker。
- ZooKeeper：外部依赖，用于保存集群元数据和 Broker 状态。

默认启动模式会在本机启动 1 个 ZKServer 和 3 个 Broker。Broker 的 RPC 端口默认为 `:7774`、`:7775`、`:7776`，Raft RPC 端口默认为 `:7331`、`:7332`、`:7333`，ZKServer RPC 端口默认为 `:7878`。

## 目录结构

```text
.
├── client/clients        # Producer 和 Consumer 客户端封装
├── cmd                  # 示例命令行客户端入口
├── docs                  # 设计说明和使用文档
├── raft                  # Raft 副本同步实现
├── server                # Broker、ZKServer、存储和订阅逻辑
├── zookeeper             # ZooKeeper 元数据访问封装
├── operations.thrift     # 业务 RPC IDL
├── raftoperations.thrift # Raft RPC IDL
└── main.go               # DongoMQ 服务启动入口
```

## 环境要求

- Linux
- Go 1.22.2 或更高兼容版本
- ZooKeeper 服务
- Kitex 命令行工具，用于根据 Thrift IDL 生成 RPC 代码

## 快速开始

### 1. 获取代码

```bash
git clone https://github.com/no-regret666/DongoMQ.git
cd DongoMQ
```

### 2. 生成 Kitex RPC 代码

如果本地没有 `kitex_gen`，或生成代码仍引用旧模块名，请重新生成 RPC 代码：

```bash
kitex -module DongoMQ operations.thrift
kitex -module DongoMQ raftoperations.thrift
```

### 3. 准备依赖

```bash
go mod download
```

### 4. 启动 ZooKeeper

DongoMQ 默认连接 `127.0.0.1:2181`，启动服务前需要先保证 ZooKeeper 可访问。也可以通过 `-zk` 参数指定其他 ZooKeeper 地址。

### 5. 启动 DongoMQ

启动 1 个 ZKServer 和 3 个 Broker：

```bash
go run . -mode all
```

也可以分别启动：

```bash
go run . -mode zkserver -zk 127.0.0.1:2181 -zkserver :7878

go run . -mode broker \
  -broker-name Broker0 \
  -broker-me 0 \
  -broker :7774 \
  -raft :7331 \
  -zkserver :7878
```

## 客户端示例

可以将示例客户端构建为 `dongomq-client`：

```bash
go build -o output/bin/dongomq-client ./cmd/...
```

生产并拉取一批测试消息：

```bash
./output/bin/dongomq-client \
  -mode demo \
  -zkserver :7878 \
  -topic phone_number \
  -part xian \
  -messages "hello DongoMQ,second message" \
  -ack 1
```

只生产消息：

```bash
./output/bin/dongomq-client \
  -mode produce \
  -zkserver :7878 \
  -topic phone_number \
  -part xian \
  -messages "hello DongoMQ" \
  -ack 1
```

只拉取消息：

```bash
./output/bin/dongomq-client \
  -mode pull \
  -zkserver :7878 \
  -topic phone_number \
  -part xian \
  -offset 0 \
  -size 10
```

## ack 模式

Producer 写入消息时可以通过 ack 参数选择 Partition 的同步方式：

| ack | 同步方式 | 返回时机 |
| --- | --- | --- |
| `-1` | Raft 同步 | 大多数副本写入成功后返回 |
| `1` | Fetch 同步 | Leader 写入成功后返回 |
| `0` | Fetch 异步同步 | Broker 收到请求后尽快返回，写入异步执行 |

使用前需要先调用 `SetPartitionState` 设置 Partition 状态，CLI 在默认 `-create=true` 时会自动创建 Topic、Partition 并设置状态。

## 构建

仓库提供了构建脚本：

```bash
bash build.sh
```

构建产物默认输出到 `output/bin/DongoMQ`。

## 测试

执行全部测试：

```bash
go test ./...
```

部分测试依赖本地文件目录、RPC 服务或 ZooKeeper 状态，运行前请确认测试环境已准备好。

## 文档

更多说明可以参考 `docs` 目录：

- `docs/producer用法.md`
- `docs/consumer用法.md`
- `docs/broker用法.md`
- `docs/zkserver用法.md`
- `docs/mq设计_meta.md`
- `docs/mq设计_zookeeper.md`

