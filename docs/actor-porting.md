# Actor 源码移植记录

移植对照基线为 Pekko commit `85ab376242152046740a63307304a2e062990f67`。Java 包前缀为
`io.github.espen7.stardust`，相对包路径、class 名称和 method 名称尽量与 Pekko 一致。

## Actor 创建

调用路径为 `ActorSystem.create`、`ActorSystemImpl.actorOf`、
`LocalActorRefProvider.actorOf`、`LocalActorRef`、`ActorCell.init`，再到 `Create`。
`ActorCell.init` 先把 `Create` 放入 child mailbox，再向 parent 投递 `Supervise`，最后调度 child mailbox；
parent 通过 `ActorCell.systemInvoke` 注册 child。对应上游 `ActorSystem.scala`、
`ActorRefProvider.scala`、`ActorRef.scala`、`ActorCell.scala` 和 `dungeon/Dispatch.scala`。

`ActorCell.contextStack` 使用 `LinkedList` 保留 Pekko thread-local List 的头插、空值哨兵和弹栈语义；
Java `ArrayDeque` 不允许 null，不能承载 Pekko 在 Actor constructor 中使用的 null marker。
`AbstractActor` 构造期间从该栈读取 ActorContext；直接使用 `new` 创建 Actor 会抛出
`ActorInitializationException`。对应上游 `Actor.scala` 的 `context` 初始化和
`ActorCell.scala` 的 `newActor`。

## 消息分发

`ActorRef.tell` 创建 `Envelope`，`Dispatcher.dispatch` 将它入队，`Mailbox.run` 优先处理 system
message，再按 throughput 处理用户消息。Mailbox 的 `Open`、`Closed`、`Scheduled` 和 suspend 位
布局与 Pekko 对齐。对应上游 `dispatch/Dispatcher.scala`、`dispatch/Mailbox.scala` 和
`dispatch/AbstractMailbox.scala`。

## 失败与 DeathWatch

`ActorCell.systemInvoke` 处理 `Supervise`、`Failed`、`Recreate`、`Terminate`、`Watch`、`Unwatch` 和
`DeathWatchNotification`。未处理的 `Terminated` 抛出 `DeathPactException`；普通未处理消息发布
`UnhandledMessage`。用户消息和 system message 路径处理 NonFatal 与 `InterruptedException`，并让
`VirtualMachineError`、`ThreadDeath` 和 `LinkageError` 继续向外传播。对应上游
`ActorCell.scala`、`Actor.scala`、`dungeon/FaultHandling.scala` 和 `dungeon/DeathWatch.scala`。

children 等待、重启 survivors、完整的挂起状态转换和所有监督策略仍未移植，不能视为 Pekko 等价实现。

## EventStream 与日志

EventStream 使用 ActorRef 作为 subscriber、class 作为 channel；订阅父类 channel 会收到子类事件，
发布通过 `ActorRef.tell` 进入 subscriber mailbox。订阅变更同步锁定 `subscriptions`；channel 查询结果保存在
volatile cache 中，发布仅在 cache miss 时获取同一把锁，对应 `SubchannelClassification` 的索引与缓存路径。
Java 使用 `HashMap` 和不可变集合快照表达上游索引及 immutable cache。终止订阅者自动清理、完整的 `LoggingBus` logger
生命周期和动态 log level 管理尚未移植。

SLF4J bridge 由系统内 logger Actor 消费 `LogEvent`，不创建专用事件分发线程。库只依赖
`slf4j-api`，不捆绑 provider。对应上游 `event/EventStream.scala`、`event/EventBus.scala`、
`event/Logging.scala` 和 `event/EventStreamUnsubscriber.scala`。

## Java 语言适配

- Scala `object` 入口转换为 Java static method。
- `@tailrec` 转换为循环；Scala `Option`、`Future`、`FiniteDuration` 分别映射为
  `Optional`、`CompletionStage`、`java.time.Duration`。
- Scala `!` 对应公开 `tell` 和内部 `sendMessage`。
- `Props` 通过 Java reflection 选择参数类型匹配的唯一 constructor；基本类型参数先映射到对应 wrapper，
  null 不匹配基本类型。多个匹配 constructor 与无匹配 constructor 都抛出 `IllegalArgumentException`。
- `ActorPath.toStringWithoutAddress` 只返回 path elements，不包含 address。

Stardust 是独立 Java 移植，不复制 Pekko 源文件。Pekko 使用 Apache-2.0 许可证。固定源码位于
[Pekko actor/src/main](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main)。
