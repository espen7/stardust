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

`ActorSystemImpl.actorOf` 通过 user guardian 的 `ActorCell.attachChild(props, name, false)`
创建顶层 Actor。`ActorCell.actorOf` 复用同一入口，先预留 child name，再调用
`ActorRefProvider`，创建失败时释放预留名称。`ActorSystemImpl` 构造期间按
`^[a-zA-Z0-9][a-zA-Z0-9-_]*$` 校验 system name，并保留上游的异常消息。

## 系统配置及 Dispatcher 创建

`ActorSystemImpl` 在初始化 runtime 组件前创建 `ActorSystem.Settings`，由后者调用
`Config.checkValid` 并解析日志、Scheduler 和系统终止使用的设置。关闭任务配置要求
`run-by-actor-system-terminate=on` 时同时启用 `terminate-actor-system`。对应上游
`actor/src/main/scala/org/apache/pekko/actor/ActorSystem.scala` 的 `Settings` 和 `ActorSystemImpl`，
以及 `actor/src/main/scala/org/apache/pekko/PekkoException.scala` 的 `ConfigurationException`。
Java 版本保留本地 runtime 已消费的 Settings 字段。旧键 `pekko.actor.loglevel` 通过
`reference.conf` 提供 `pekko.loglevel` 的回退值。有限的 dead-letter 暂停配置按上游转换为毫秒，
无限期以 `null` 表示。日志沿用原配置文本，Pekko 使用 Scala Duration 的 `pretty` 格式。

系统 Dispatcher 通过 `Dispatchers.defaultGlobalDispatcher`、`lookup` 和
`DispatcherConfigurator.dispatcher` 创建。命名配置继承默认配置段，别名解析以 20 层为限。
按上游用 `ConcurrentHashMap.putIfAbsent` 缓存 configurator。Stardust 的 Dispatcher
在构造时创建 ForkJoinPool，因此竞争中落败的实例会关闭。ActorSystem 终止时关闭注册表中的实例。
对应上游 `actor/src/main/scala/org/apache/pekko/dispatch/Dispatchers.scala`、
`dispatch/Dispatcher.scala`、`dispatch/AbstractDispatcher.scala` 和
`actor/src/main/resources/reference.conf`。

当前支持普通 `Dispatcher` 的 `throughput` 与 `throughput-deadline-time`。Actor 创建统一使用系统默认
Dispatcher。独立的 internal dispatcher、`Props.withDispatcher` 绑定、PinnedDispatcher、自定义 configurator、
executor 配置、按空闲超时释放线程池和 `CachingConfig` 优化仍待移植。现有 Java 无参 Dispatcher
构造器从 `reference.conf` 读取默认 throughput。

## 消息分发

`ActorRef.tell` 创建 `Envelope`，`Dispatcher.dispatch` 将它入队，`Mailbox.run` 优先处理 system
message，再按 throughput 批量处理用户消息。每条普通消息处理完并清理系统消息后，Mailbox 检查批次截止时间。
Mailbox 的 `Open`、`Closed`、`Scheduled` 和 suspend 位
布局与 Pekko 对齐。对应上游 `dispatch/Dispatcher.scala`、`dispatch/Mailbox.scala` 和
`dispatch/AbstractMailbox.scala`。

## 失败与 DeathWatch

`ActorCell.systemInvoke` 处理 `Supervise`、`Failed`、`Recreate`、`Terminate`、`Watch`、`Unwatch` 和
`DeathWatchNotification`。未处理的 `Terminated` 抛出 `DeathPactException`；普通未处理消息发布
`UnhandledMessage`。用户消息和 system message 路径处理 NonFatal 与 `InterruptedException`，并让
`VirtualMachineError`、`ThreadDeath` 和 `LinkageError` 继续向外传播。对应上游
`ActorCell.scala`、`Actor.scala`、`dungeon/FaultHandling.scala` 和 `dungeon/DeathWatch.scala`。

`ChildrenContainer` 对应 `actor/.../actor/dungeon/ChildrenContainer.scala` 的
`EmptyChildrenContainer`、`TerminatedChildrenContainer`、`NormalChildrenContainer` 和
`TerminatingChildrenContainer`。各容器保留上游 `add`、`remove`、`getByName`、`getByRef`、
`children`、`stats`、`shallDie`、`reserve` 和 `unreserve` 的状态转换；最后一个待终止 child 移除后，
`ActorCell.handleChildTerminated` 继续 `finishCreate`、`finishRecreate` 或 `finishTerminate`。
Java 将 Scala companion object 内的类型作为 `ChildrenContainer` 的嵌套类型表达。

`ChildRestartStats` 对应 `actor/.../actor/FaultHandling.scala`，其 `uid`、有限重试计数、无限重试、
时间窗累计及过期重置逻辑与上游一致。纳秒时间通过 `System.nanoTime()`，时间窗配置按上游转为毫秒。
`SupervisorStrategy`、`OneForOneStrategy` 和新增的 `AllForOneStrategy` 对齐同文件的
`handleFailure`、`processFailure`、`logFailure` 和 `restartChild` 职责。Java 使用
`Function<Throwable, Directive>` 表达 decider，并以负 `Duration` 表示 Pekko `Duration.Inf`。
未匹配异常返回 `Escalate`，由 `ActorCell.handleFailure` 原样向上抛出；默认策略对
`ActorInitializationException`、`ActorKilledException` 和 `DeathPactException` 执行 Stop，普通
`Exception` 执行 Restart。此批仍未移植顶层 guardian 的 strategy configurator 和 Pekko 的所有 Java
构造重载，也不代表通过 Pekko 完整测试套件。

## 本地 ask 与一次性调度

`Patterns.ask` 通过 `AskableActorRef.internalAsk` 检查目标类型、终止状态和 timeout，之后由
`PromiseActorRef` 先注册超时再投递请求。普通回复、Classic `Status.Success` / `Status.Failure` 和超时
竞争完成结果。结果完成时停止临时引用并取消计时任务。非正 timeout 通过失败的 `CompletionStage` 返回，
已经终止的内部引用先收到一次普通投递，再立即返回失败。对应
`actor/src/main/scala/org/apache/pekko/pattern/Patterns.scala`、
`actor/src/main/scala/org/apache/pekko/pattern/AskSupport.scala` 和
`actor/src/main/scala/org/apache/pekko/actor/Actor.scala`。

`PromiseActorRef` 使用 VarHandle CAS 表达上游的 path 与 watcher 状态。Java 用 `Set.copyOf` 快照表达
Scala immutable Set。Provider 分配带目标路径前缀的临时路径。首次读取存活引用的 path 时才注册，
停止时回收已注册的路径并通知 watcher。迟到或重复的回复通过 `DeadLetterActorRef` 发布事件。
Actor 注销时将 Mailbox 替换为共享 dead-letter Mailbox。对应 `actor/ActorRef.scala`、
`actor/ActorRefProvider.scala`、`actor/dungeon/DeathWatch.scala`、`dispatch/AbstractDispatcher.scala`、
`dispatch/Mailboxes.scala` 和 `dispatch/Mailbox.scala`。本地 Actor 和 dead letters 已处理
`Identify` / `ActorIdentity`；ActorSelection 消息及引用序列化仍未移植。

`DeadLetterListener` 订阅 `DeadLetter`、`Dropped` 和 `UnhandledMessage`，按上游
`pekko.log-dead-letters`、`pekko.log-dead-letters-suspend-duration` 和
`pekko.log-dead-letters-during-shutdown` 的默认值限制日志。对应
`actor/src/main/scala/org/apache/pekko/event/DeadLetterListener.scala` 和 `actor/ActorSystem.scala`。
本地实现已加入独立的 system guardian，系统 Actor 由 `systemActorOf` 挂载到
`/system`，并在 user guardian 终止后完成 system guardian 的终止通知。日志事件提供
`LogEventWithMarker` 和 `LogMarker`，SLF4J bridge 会将 marker 属性写入 MDC 后清理。
`CoordinatedShutdown` 已实现配置阶段 DAG、阶段超时、并行任务、recover、取消句柄和
`ActorSystem.terminate()` 的最后终止任务，并按配置注册可取消的 JVM shutdown hook；
`System.exit`、集群任务和远程 Actor 的扩展仍未移植。`ActorSelectionMessage` 的 dead-letter 特殊处理仍在
ActorSelection 范围之外。
`ActorSelectionMessage` 的 dead-letter 特殊处理仍在 ActorSelection 范围之外。

一次性调度移植 `actor/LightArrayRevolverScheduler.scala` 和
`dispatch/AbstractNodeQueue.java` 中对应的时间轮、发布队列、取消和关闭路径。关闭时仅运行
待执行的 `Scheduler.TaskRunOnClose`。系统关闭时尚未触发的普通 ask 超时任务会丢弃，
对应结果仍可能处于未完成状态。配置位于
`pekko.scheduler`，默认 10ms tick、512 个时间轮槽和 5s 停止等待时间。
周期调度及完整 Dispatcher shutdown 仍待移植。Java 使用 `Executor` 对应 Scheduler 的
`ExecutionContext`，现有无 Executor 重载在触发任务的线程执行。应用通过
`ActorSystem.scheduler()` 取得 Scheduler。自定义实现通过 `pekko.scheduler.implementation`
配置，使用接收 `Config`、`LoggingAdapter` 和 `ThreadFactory` 的公开构造方法。
由于 Java `Executor` 没有 `reportFailure`，新增 `dispatch.ExecutionContext` 提供对应接口；
传入普通 `Executor` 时无法直接表达上游回调，失败由当前线程的 uncaught exception handler 报告。
对应上游 `actor/LightArrayRevolverScheduler.scala` 中 `TaskHolder.executeTask` 和
`notifyCancellation`。该适配与 Scala ExecutionContext 的报告实现不保证一致。

ask 使用 `CompletableFuture` 保存结果，并返回只读的 `CompletionStage` 视图。回调调度遵循 Java
`CompletionStage` 规则，Pekko 的 Scala `Future` / `ExecutionContext.parasitic` 回调语义未移植。
毫秒重载继续返回 Java `CompletionStage`。临时引用和 dead-letter 引用由 runtime 创建，
旧的手动构造入口已移除。
`PromiseActorRef` 已补充回复后的 `onComplete`、超时后的 `onTimeout` 和非法 Unwatch 分支；
对应上游 `actor/src/main/scala/org/apache/pekko/pattern/AskSupport.scala`。两个钩子在该基线中为空实现。

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
