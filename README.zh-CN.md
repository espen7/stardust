# Tapio

**面向高并发分布式系统的轻量级 Java 25 Actor 平台。**

[![Java 25](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.org/projects/jdk/25/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Tapio 是对 Akka 2.6 和 Apache Pekko 背后的 Classic Actor 模型进行 Java 原生重写的项目。它以现代 Java 为基础，提供小型 runtime、面向虚拟线程的并发设计，以及面向游戏和其他高并发业务的精选分布式组件。

Tapio 希望做到易学、易嵌入，也方便 Java 开发者从公开 API 一直阅读到 runtime 实现。项目保留 Akka 生态中真正有用的部分，同时不把所有服务都需要的大量集成和抽象带进来。

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## Akka、Pekko 与 Tapio

Akka 2.6 是 Classic Actor 模型的 API 和设计源流。Apache Pekko 是 Akka 在项目方向变化之后形成的 Apache-2.0 社区 fork 和延续项目。Tapio 是受这一脉络启发的独立 Java 实现：

- 保留 `ActorSystem`、`ActorRef`、Mailbox、supervision、EventStream 和 `ask` 等熟悉的 Actor 概念；
- 以 Java 25 API 为基础，面向现代并发设计 runtime，其中包括虚拟线程方向；
- 有意识地缩小范围，不追求完整复制 Akka 或 Pekko 的全部生态。

Tapio 与 Lightbend、Apache Pekko 项目没有隶属关系，也不是 Akka 或 Pekko 的二进制兼容替代品。

## 为什么选择 Tapio

- **Java 原生**：应用代码和 runtime 开发都不要求 Scala 工具链。
- **现代并发**：以 Java 25 为基线，把虚拟线程作为可扩展业务的核心方向。
- **小而聚焦**：只保留 Actor runtime 和实际服务需要的分布式组件。
- **源码易读**：清晰的 Java 实现和熟悉的 Classic Actor 模型，适合学习和源码分析。
- **面向高并发**：为游戏服务器、网关、仿真服务和其他有状态业务提供基础。

## 范围与状态

Tapio 目前处于早期开发阶段。第一个稳定版本发布前，公开 API 和配置仍可能调整。

| 领域 | 状态 |
| --- | --- |
| 本地 Classic Actor runtime | `tapio-actor` 已提供 |
| Actor 生命周期、Mailbox、Dispatcher、supervision、DeathWatch | `tapio-actor` 已提供 |
| EventStream、结构化日志、`ask`、一次性调度 | `tapio-actor` 已提供 |
| 路由（Routing） | 计划中 |
| Cluster Sharding | 计划中 |
| Kubernetes 集群发现 | 计划中 |
| 集群功能所需的传输和成员关系组件 | 作为 Tapio 聚焦组件计划实现 |

首期范围不包含完整的 Akka/Pekko 生态：Typed API、Streams、Persistence、大范围序列化集成、完整 Remote 兼容，以及无关的企业集成。只有直接服务于 Actor 平台和目标业务的组件，才会进入后续范围。

## 快速开始

### 环境要求

- JDK 25
- Maven 3.9 或更新版本

### 从源码构建

```shell
git clone https://github.com/espen7/tapio.git
cd tapio
mvn test
```

不运行测试直接打包 Actor 模块：

```shell
mvn -DskipTests package
```

### 最小 Actor 示例

```java
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.pattern.Patterns;

import java.time.Duration;

public final class EchoActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, message -> getSender().tell(message, getSelf()))
                .build();
    }

    public static void main(String[] args) {
        try (ActorSystem system = ActorSystem.create("example")) {
            ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");
            Object reply = Patterns.ask(echo, "hello", Duration.ofSeconds(1))
                    .toCompletableFuture()
                    .join();
            System.out.println(reply);
        }
    }
}
```

当前仓库从源码构建。首个版本发布后，README 会补充正式的依赖坐标。

## 架构

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem、ActorRef、生命周期、supervision
    │   ├── dispatch/    # Dispatcher、Mailbox、Envelope、system message
    │   ├── event/       # EventStream、LogEvent、SLF4J bridge
    │   ├── japi/        # Java Creator 和 ReceiveBuilder API
    │   └── pattern/     # ask 模式
    └── resources/       # reference.conf 默认配置
```

`tapio-actor` 当前提供本地 runtime。路由、Sharding 和 Kubernetes 发现会围绕同一 Actor 模型作为聚焦模块加入，不要求引入完整 Pekko 生态。

## 配置与日志

`ActorSystem.create(name)` 会加载 `application.conf` 和模块的 `reference.conf` 默认值。runtime 通过 `ActorSystem` 提供命名 Dispatcher 查找、Scheduler 设置、协调关闭、dead-letter 日志和基于 SLF4J 的结构化事件。

Tapio 只依赖 `slf4j-api`。应用可以选择并配置 Logback、Log4j2、SLF4J Simple 或 JUL provider。

详细配置说明和源码设计记录见 [docs/actor-porting.md](docs/actor-porting.md)。

## 路线图

1. 完成本地 Actor runtime 并稳定公开 API。
2. 添加适用于常见服务和游戏服务器拓扑的路由能力。
3. 添加集群成员关系和 Kubernetes 发现。
4. 添加 Cluster Sharding，支持有状态 Actor 横向扩展。
5. 在真实高并发负载下验证 Java 25 和虚拟线程执行模型。

## 参与贡献

欢迎提交 Issue 和 Pull Request。较大的 API 或 runtime 改动建议先开 Issue，先讨论行为和兼容性影响。新增 runtime 行为应包含精简的 JUnit 5 覆盖；公共契约发生变化时，也请同步更新文档。

## 源码渊源与许可证

Tapio 使用 [Apache License 2.0](LICENSE) 发布。Actor runtime 遵循 Classic Actor 语义，并以 Apache Pekko 作为实现参考。源码映射、Java 适配说明和适用的上游 NOTICE 记录在 [docs/actor-porting.md](docs/actor-porting.md) 以及源码头部。

Tapio 是独立项目，与 Lightbend、Akka 或 Apache Pekko 没有背书或隶属关系。
