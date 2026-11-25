# Tapio

Tapio 是一个使用 Java 25 阅读并逐步移植 Apache Pekko Classic Actor 本地运行时的项目。实现以 [Pekko 提交 `85ab376242`](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main) 为对照，尽量保留上游的类名、方法名和调用顺序，便于在两份代码之间逐段阅读。

Tapio 当前处于早期移植阶段，适合源码学习和继续实现，尚未提供 Pekko 的完整功能集。

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## 范围

当前模块为 `tapio-actor`，包含本地 Actor 创建、消息 Envelope、Mailbox 和 Dispatcher、基础生命周期、基础 One-for-One 监督、DeathWatch 消息、`Patterns.ask`、一次性调度和初步日志事件支持。

以下部分仍与上游存在差异：

- `ChildrenContainer` 已按 Pekko 风格拆分为空、普通、等待终止和已终止状态，并在 restart 和 termination 后继续处理 lifecycle。`ChildRestartStats` 已实现 child UID、重试次数和时间窗统计。
- 监督路径已支持 Pekko 默认异常分类、One-for-One 与 All-for-One、重试限制、升级和失败日志；完整的 guardian 监督生命周期仍未移植。
- 一次性调度使用 `LightArrayRevolverScheduler`，配置位于 `pekko.scheduler`。周期调度 API 尚未移植。
- 本地 `Patterns.ask` 使用系统 Scheduler 和临时 `PromiseActorRef`，支持 Classic `Status.Success` / `Status.Failure`、临时引用的 DeathWatch、迟到回复的 dead letters，以及返回 Java `CompletionStage` 的毫秒重载。
- EventStream 使用 ActorRef 订阅者和 class channel，并按 channel 的父子类型关系分发。默认 dead letters 日志订阅器支持计数和暂停；system guardian、`systemActorOf`、`CoordinatedShutdown` 阶段关闭、JVM shutdown hook 和 `LogMarker`/MDC bridge 已加入本地 runtime。System.exit、集群任务和完整日志服务仍未移植。receive timeout、ActorSelection、Stash、路由、序列化、I/O、Typed、Remote 和 Sharding 尚未实现。

每个上游概念的源码位置和 Java 对应关系记录在 [docs/actor-porting.md](docs/actor-porting.md)。

## 环境要求

- JDK 25
- Maven 3.9 或更新版本

当前仓库没有 Maven Wrapper。确认 `java -version` 和 `mvn -version` 后，在仓库根目录执行：

```shell
mvn test
```

单元测试覆盖本地运行时的基础调用路径，不代表行为已通过 Pekko 测试套件验证。打包模块可执行 `mvn -DskipTests package`。

## ActorSystem 配置

`ActorSystem.create(name)` 加载 `application.conf` 和 `reference.conf`。`ActorSystem.create(name, config)` 使用传入的配置，并由 `reference.conf` 补齐默认值。有效配置可通过 `system.settings().config()` 读取。

`pekko.actor.default-dispatcher.throughput` 默认是 `5`，控制一次 Mailbox 调度处理的普通消息数。
`pekko.actor.default-dispatcher.throughput-deadline-time` 默认是 `0ms`，表示关闭批次时间限制。
`system.dispatchers().lookup(id)` 支持查找命名的普通 Dispatcher，多个别名可以指向同一个实例。Actor 创建目前使用默认 Dispatcher。

## Logging

`ActorSystem` 和 `AbstractActor` 提供 `log()`，支持 `debug`、`info`、`warning`、`error` 和 `{}` 参数模板。日志先成为带 source path、source class、level、message 和 cause 的 `LogEvent`，再发布到 `ActorSystem.eventStream()`。默认 SLF4J logger Actor 会将 source path 加在消息前；应用可以使用 ActorRef 和 `LogEvent.class` 订阅结构化事件。

运行库只依赖 `slf4j-api`，不捆绑日志实现。应用需要在运行时添加一个 SLF4J provider，例如 Logback、Log4j2、SLF4J Simple 或 JUL provider，并配置其级别和输出格式。未添加 provider 时，SLF4J 会提示缺少 provider 并使用 NOP logger。

ActorSystem 从 `application.conf` 读取与 Pekko 对齐的日志设置：

```hocon
pekko.loglevel = "DEBUG"
pekko.actor.debug.lifecycle = on
pekko.actor.debug.unhandled = on
```

要输出生命周期和未处理消息的 DEBUG 日志，需要将 `pekko.loglevel` 设为 `DEBUG`，并在所选 SLF4J provider 中为相关 logger 启用 DEBUG 级别。兼容旧版的 `pekko.actor.loglevel` 键仍可作为回退值。生命周期日志包括 Actor 创建、监督、restart、stop 和 DeathWatch 变化，默认关闭。未处理消息始终作为 `UnhandledMessage` 发布；设为 `debug.unhandled = on` 后才会转成 DEBUG 日志。`debug.receive` 和 `debug.autoreceive` 键已保留，但相关 receive decorator 和 AutoReceive API 尚未移植。

## 模块结构

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem、ActorRef、ActorCell、生命周期
    │   ├── dispatch/    # Dispatcher、Mailbox、Envelope、SystemMessage
    │   ├── event/       # LogEvent、EventStream、LoggingAdapter 和 SLF4J bridge
    │   ├── japi/        # Java Creator 和 ReceiveBuilder
    │   └── pattern/     # ask 模式
    └── resources/
        └── reference.conf
```

Java 包保留 Pekko 的相对层级，并使用 `io.github.espen7.tapio` 作为包前缀。入口和类的映射以 Pekko 提交 `85ab376242` 为准。

## 许可证

Tapio 按 Apache License 2.0 发布，详情见 [LICENSE](LICENSE)。实现应继续记录 Pekko 对照来源，并为直接派生或复制的代码保留适用的版权、许可和 NOTICE 信息。
