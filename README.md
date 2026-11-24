# Stardust

Stardust 是一个用 Java 25 阅读并逐步重写 Apache Pekko Classic Actor 本地运行时的项目。实现以 [Pekko 源码提交 `85ab376242`](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main) 为对照，尽量保留上游类名、方法名和调用顺序，便于在两份源码之间逐段阅读。

当前仓库处于早期迁移阶段。它适合源码学习和继续实现，尚未达到 Pekko 的功能完整度，也不应作为 Pekko 的生产替代品。

## 范围

当前模块为 `stardust-actor`，包含本地 Actor 创建、消息 Envelope、Mailbox 和 Dispatcher、基础生命周期、基础 One-for-One 监督、DeathWatch 消息、`Patterns.ask`、一次性调度和初步日志事件支持。

这些部分仍与上游有明显差异：

- `ChildrenContainer` 只覆盖名字预留、child 注册和基础终止状态。Pekko 的等待子 Actor、重启 survivors 和完整状态转换尚未移植。
- 监督路径已按 child 向 parent 发送 `Failed` 的方式组织，但 Pekko 的递归 suspend、失败状态、重启期间等待 children、异常分类和重试策略尚未完整实现。
- `Scheduler` 目前使用 `ScheduledExecutorService`。Pekko 的 `LightArrayRevolverScheduler` 时间轮、tick 和关闭语义尚未移植。
- `Patterns.ask` 使用独立计时器和临时 `PromiseActorRef`，尚未接入 Provider 的 temp path 注册与回收。
- EventStream 目前只提供基本订阅和日志事件发布；Pekko 的 DeadLetter event、receive timeout、ActorSelection、Stash、路由、序列化、I/O、Typed、Remote 和 Sharding 尚未完成。

每个上游概念的源码位置和 Java 对应关系记录在 [docs/actor-porting.md](docs/actor-porting.md)。

## 环境要求

- JDK 25
- Maven 3.9 或更新版本

当前仓库没有 Maven Wrapper。确认 `java -version` 和 `mvn -version` 后，在仓库根目录执行：

```shell
mvn test
```

单元测试覆盖本地运行时的基础调用路径，不代表行为已通过 Pekko 测试套件验证。打包模块可执行 `mvn -DskipTests package`。

## Logging

`ActorSystem` 和 `AbstractActor` 提供 `log()`，支持 `debug`、`info`、`warning`、`error` 和 `{}` 参数模板。日志先成为带 source path、source class、level、message 和 cause 的 `LogEvent`，再发布到 `ActorSystem.eventStream()`。默认 SLF4J bridge 将 source path 加在消息前；应用可自行订阅结构化事件。

运行库只依赖 `slf4j-api`，不捆绑日志实现。应用需要在运行时添加一个 SLF4J provider（例如 Logback、Log4j2、SLF4J Simple 或 JUL provider），并由应用配置其级别和输出格式。未添加 provider 时，SLF4J 会提示并使用 NOP logger。

ActorSystem 从 `application.conf` 读取 Pekko 对齐的日志设置：

```hocon
pekko.actor.loglevel = "DEBUG"
pekko.actor.debug.lifecycle = on
pekko.actor.debug.unhandled = on
```

要输出生命周期和未处理消息的 DEBUG 日志，还需将 `pekko.actor.loglevel` 设为 `DEBUG`，并在所选 SLF4J provider 中启用相应 logger 的 DEBUG 级别。生命周期日志包括 Actor 创建、监督、restart、stop 和 DeathWatch 变化，默认关闭。未处理消息始终作为 `UnhandledMessage` 发布；设为 `debug.unhandled = on` 后才会转成 DEBUG 日志。`debug.receive` 和 `debug.autoreceive` 键已保留，相关 receive decorator 和 AutoReceive API 尚未移植。

## 模块结构

```text
stardust-actor/
└── src/main/
    ├── java/io/github/espen7/stardust/
    │   ├── actor/       # ActorSystem、ActorRef、ActorCell、生命周期
    │   ├── dispatch/    # Dispatcher、Mailbox、Envelope、SystemMessage
    │   ├── event/       # LogEvent、EventStream、LoggingAdapter 和 SLF4J bridge
    │   ├── japi/        # Java Creator 和 ReceiveBuilder
    │   └── pattern/     # ask 模式
    └── resources/
        └── reference.conf
```

Java 包保留 Pekko 的相对层级，并使用 `io.github.espen7.stardust` 作为包前缀。入口和类的映射以 Pekko 提交 `85ab376242` 为准。

## 许可证

Stardust 按 Apache License 2.0 发布，详情见 [LICENSE](LICENSE)。实现应继续记录 Pekko 对照来源，并为直接派生或复制的代码保留适用的版权、许可和 NOTICE 信息。
