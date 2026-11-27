# Actor 源码移植记录

移植对照基线为 Pekko commit `85ab376242152046740a63307304a2e062990f67`。Java 包前缀为
`io.github.espen7.tapio`，相对包路径、class 名称和 method 名称尽量与 Pekko 一致。
`ActorPath.Scheme` 统一提供 `tapio` ActorPath scheme 和 runtime 配置根；内部 `@InternalApi` 常量类 `io.github.espen7.tapio.actor.ConfigKeys` 集中收口 `tapio.*` 配置路径常量（Pekko 上游在各处直接内联 `"pekko.*"` 字符串，此常量表仅作 Java 侧内部配置键管理，不构成对外 Pekko 契约）。
本记录按迁移批次更新；“本批对账完成”只表示所列契约已有实现和定向测试，不代表整个 Classic Actor
迁移完成，也不代表通过 Pekko 完整测试套件或达到全量源码 parity。

## Actor 创建

`ActorSystem` 和 `ActorContext` 共用 `ActorRefFactory` 的 `actorOf`、`stop` 和
`dispatcher`。`ActorSystemImpl.actorOf` 通过 user guardian 的 `attachChild` 创建应用 Actor，
`systemActorOf` 通过 system guardian 挂载系统 Actor。两条路径与 `ActorCell.actorOf` 一起进入
`ActorCell.makeChild`。`makeChild` 预留名称、分配带 incarnation UID 的路径，再调用
`ActorRefProvider.actorOf` 创建引用。创建失败时释放预留名称。`ActorCell.init` 只创建并绑定
mailbox、放入 `Create`，外部挂载时通知 parent `Supervise`；`LocalActorRef.start` 或
`RepointableActorRef.point` 随后调用 `ActorCell.start`，经 `Dispatcher.attach` 增加 inhabitants
并首次调度 mailbox。对应上游 `ActorSystem.scala`、`ActorRefProvider.scala`、`ActorRef.scala`、
`ActorCell.scala`、`dungeon/Children.scala` 和 `dungeon/Dispatch.scala`。

`ActorCell.contextStack` 使用 `LinkedList` 保留 Pekko thread-local List 的头插、空值哨兵和弹栈语义；
Java `ArrayDeque` 不允许 null，不能承载 Pekko 在 Actor constructor 中使用的 null marker。
`AbstractActor` 构造期间从该栈读取 ActorContext；直接使用 `new` 创建 Actor 会抛出
`ActorInitializationException`。对应上游 `Actor.scala` 的 `context` 初始化和
`ActorCell.scala` 的 `newActor`。

自动名称使用每个 parent 的计数器和上游 `util/Helpers.scala` 中以 `$` 开头的编码。
Java 的 `InvalidActorNameException` 继承原入口抛出的 `IllegalArgumentException`，
保留现有调用方的异常处理。ActorSystem 停止顶层 Actor 时向对应 guardian 发送
`StopChild`。通过 `ActorContext` 停止 child 时由当前 cell 更新 children 状态。
`ActorSystemImpl` 按 `^[a-zA-Z0-9][a-zA-Z0-9-_]*$` 校验 system name。

本地 Provider 在 `/` 创建独立的 root guardian，先启动根 Actor，再预留、注册并启动
`/user` 和 `/system` 两个直接子 Actor。`/temp` 和 `/deadLetters` 由根引用的
`getSingleChild` 直接返回，对应 `actor/src/main/scala/org/apache/pekko/actor/ActorRefProvider.scala`。
根引用在路径遍历中以自身为 parent，在 `ActorCell` 内由 `/bubble-walker` 监督。
Java 继续使用现有 `ActorPath` 类表示根路径与子路径，并用 `null` 表示内部单级查找未命中。
内置 root、user 和 system guardian 均使用 `tapio.actor.internal-dispatcher`，应用 Actor 仍按
Props、部署配置或默认配置选择 Dispatcher。对应上游 `actor/ActorPath.scala`、`actor/ActorRef.scala`、
`actor/dungeon/Children.scala`、`actor/dungeon/Dispatch.scala` 和 `actor/ActorRefProvider.scala`。
额外顶级名称注册和 Remote 仍待移植。`ActorSystemImpl` 已支持内部 `guardianProps` 自定义 user guardian，按上游在自定义 guardian 下拒绝外部 `actorOf` 并为 `/user` 选择独立 Dispatcher 与 Mailbox。

Provider 的 `rootGuardianAt`、`getDefaultAddress` 和 `getExternalAddressFor` 按本地
`Address` 匹配。两个 `resolveActorRef` 重载从 root guardian 查询。本地有效路径未命中时返回保留
目标路径的 `EmptyLocalActorRef`，URI 无法解析或地址不匹配时返回系统死信引用。
对应上游 `actor/Address.scala`、`actor/ActorPath.scala`、`actor/ActorRef.scala` 和
`actor/ActorRefProvider.scala`。Provider 的 `terminationFuture` 用只读 `CompletionStage` 表达，
`scheduler()` 仍为 Java ask 实现取得系统调度器。Provider 按上游缓存
`Serialization.Information`，保存本地根地址和当前 ActorSystem。完整序列化扩展、
传输上下文和 ActorRef 序列化仍待移植。

`ActorContext.actorOf` 使用 `LocalActorRef`。`ActorSystem` 的外部挂载使用
`RepointableActorRef`，由 supervisor 处理 `Supervise` 时激活。`UnstartedCell` 使用上游的
`ReentrantLock`、`LinkedList` 和侵入式系统消息链表。先切换子引用查询入口，再启动真实 cell，
最后在锁内排空缓存并切换消息投递入口。异步 mailbox 创建失败时使用默认 dispatcher 和无界邮箱，
由监督流程处理初始化异常。`tapio.actor.unstarted-push-timeout` 默认 10 秒，对应上游
`actor/RepointableActorRef.scala`、`actor/dungeon/Dispatch.scala` 和 `actor/src/main/resources/reference.conf`。
`ActorCell.swapMailbox` 使用 VarHandle 的 volatile read 和 compareAndSet 重试循环，保留上游
CAS 替换语义。临时队列锁等待被中断时，
借用现有的泛型重抛机制传播原 `InterruptedException`。

`ActorRefFactory.actorSelection` 支持相对路径、绝对路径和带地址的 ActorPath。系统入口
以根引用为锚点，context 的相对路径以 self 为锚点。选择支持父级、精确子名称和通配符，
并通过 `Identify` 实现 `resolveOne`。未命中的本地选择按上游处理 Identify、dead letters 和
wildcard fan-out。`Patterns.ask` 复用 `PromiseActorRef`，等待首个回复。对应上游
`actor/ActorSelection.scala`、`actor/ActorRefProvider.scala`、`actor/ActorCell.scala`、
`actor/ActorRef.scala`、`pattern/AskSupport.scala` 和 `util/Glob.scala`。
与上游一样，factory 的字符串参数使用 URI 解析，问号可能被解释为 query，显式锚点版本
`ActorSelection.apply(anchor, path)` 可传入含问号的通配符。Java hashCode 使用 anchor 和路径元素，
省略上游 MurmurHash 混合。远程选择和选择消息的传输序列化仍待移植。

## 系统配置及 Dispatcher 创建

`ActorSystemImpl` 在初始化 runtime 组件前创建 `ActorSystem.Settings`，由后者调用
`Config.checkValid` 并解析日志、Scheduler 和系统终止使用的设置。随后按
`Settings/EventStream/Log → Scheduler → Provider → Mailboxes → Dispatchers → 默认 Dispatcher →
CoordinatedShutdown → Provider.init` 的顺序启动。Provider 构造只创建 deadLetters 等内部引用，
root/user/system guardian 延后到 `init` 创建，避免 Provider、Mailboxes 和 Dispatcher 的构造循环依赖；
启动失败时若 guardian 尚未存在，则直接关闭已创建的 Scheduler 与 Dispatcher；若已发起 guardian 停止，
则保留处理终止消息所需的资源，由终止回调收尾。
关闭任务配置要求 `run-by-actor-system-terminate=on` 时同时启用 `terminate-actor-system`。对应上游
`actor/src/main/scala/org/apache/pekko/actor/ActorSystem.scala` 的 `Settings` 和 `ActorSystemImpl`，
以及 `actor/src/main/scala/org/apache/pekko/PekkoException.scala` 的 `ConfigurationException`。
Java 版本将项目自身的异常基类命名为 `TapioException`；上游 `PekkoException.scala` 仅作为对照来源保留。
Java 版本保留本地 runtime 已消费的 Settings 字段。`tapio.actor.loglevel` 通过
`reference.conf` 提供 `tapio.loglevel` 的回退值。有限的 dead-letter 暂停配置按上游转换为毫秒，
无限期以 `null` 表示。日志沿用原配置文本，Pekko 使用 Scala Duration 的 `pretty` 格式。

系统 Dispatcher 通过 `Dispatchers.defaultGlobalDispatcher`、`lookup` 和
`MessageDispatcherConfigurator.dispatcher` 创建。命名配置继承默认配置段，别名解析以 20 层为限。
按上游用 `ConcurrentHashMap.putIfAbsent` 缓存 configurator；竞争落败的 lazy 实例没有创建
executor，不再额外关闭。`DispatcherConfigurator` 每次返回同一个普通 Dispatcher，
`PinnedDispatcherConfigurator` 每次返回新实例；别名共享目标 configurator，因此 Pinned 别名也会
每次创建独立实例。`Dispatchers` 构造时先创建唯一的 internal Dispatcher，供内置 guardian 使用。
注册表不在 ActorSystem 终止时批量关闭实例，实例由各自的 idle shutdown action 释放。对应上游
`actor/src/main/scala/org/apache/pekko/dispatch/Dispatchers.scala`、`dispatch/Dispatcher.scala`、
`dispatch/AbstractDispatcher.scala` 和 `actor/src/main/resources/reference.conf`。

Dispatcher 层级恢复为 `BatchingExecutor → AbstractMessageDispatcher → MessageDispatcher →
Dispatcher / PinnedDispatcher`。`MessageDispatcher` 负责 inhabitants、attach/detach、suspend/resume、
unbatched `TaskInvocation` 和延迟 shutdown；`Dispatcher` 负责 mailbox 调度、lazy
`ExecutorServiceDelegate`、普通 executor 配置与重试。上游 Java 版 `AbstractMessageDispatcher` 与
`BatchingExecutor` 的组合关系在 Java 单继承下合并为前者继承后者，字段名、三态常量和 VarHandle
名称保持一致。

普通 `Dispatcher` 使用 lazy `ExecutorService` delegate。首次提交 mailbox 或普通 Runnable 才按
`executor` 配置创建 ForkJoinPool 或 ThreadPoolExecutor；`shutdown` 先原子换入新的 delegate，再关闭旧
executor，因此后续任务可以创建并使用新 executor。inhabitants 统计已注册 Actor 和正在执行的普通
Runnable，mailbox 提交不重复计数。
`attach` 先注册 Actor 再调度 mailbox，`detach` 先注销并关闭、清理旧 mailbox，最后在 inhabitants
归零时安排延迟 shutdown。

延迟 shutdown 使用 `UNSCHEDULED=0`、`SCHEDULED=1`、`RESCHEDULED=2` 三态 CAS。已有
shutdown action 时再次归零只会标记 `RESCHEDULED`；action 看到该状态会重新安排，而不是关闭仍在
使用的 executor。shutdown action 实现 `Scheduler.TaskRunOnClose`，Scheduler 关闭时会执行；若
Scheduler 已关闭导致无法安排，则立即 shutdown 并恢复 `UNSCHEDULED`。对应上游
`dispatch/AbstractDispatcher.scala` 的 inhabitants、attach/detach 和 shutdown action。

`tapio.actor.default-dispatcher.executor` 支持 `default-executor`、`fork-join-executor` 和
`thread-pool-executor`。默认 executor 优先使用 `DispatcherPrerequisites.defaultExecutionContext`，
发布与上游一致的 DEBUG 事件并通过不接管外部生命周期的 `ExecutorService` 适配器委托；Java 将
上游匿名 mixin 适配为私有具名类 `ExecutionContextExecutorService`。未提供时通过同一配置段内的
`default-executor.fallback` 回退；
三种 executor 的参数段均嵌在 `default-dispatcher` 下，与上游 `reference.conf` 层级一致。
fork-join 按 `min(max(ceil(availableProcessors × factor), minimum), maximum)` 计算 parallelism，
`FIFO` 对应 async mode、`LIFO` 对应 sync mode，并传递 `maximum-pool-size`。`minimum-runnable`
在 JDK 25+ 且配置为负数时解析为 `min(8, max(1, parallelism / 2))`，旧 JDK 为 `1`；非负配置原样使用。
thread-pool 支持 fixed pool 或 core/max 两组 factor 缩放、毫秒 keep-alive、`allow-core-timeout`、
无界 linked queue、有界 linked/array queue；只有 queue size 大于 0 时才校验 `task-queue-type`。
饱和时 `SaneRejectedExecutionHandler` 在提交线程执行任务，executor 已关闭时抛出
`RejectedExecutionException("Shutdown")`。对应上游 `dispatch/AbstractDispatcher.scala`、
`dispatch/ForkJoinExecutorConfigurator.scala`、`dispatch/ThreadPoolBuilder.scala` 和
`actor/src/main/resources/reference.conf`。`DispatcherExecutorConfigTest` 覆盖默认回退、显式 executor、
池大小、队列、拒绝策略、线程工厂、JDK 25 minimum-runnable、ForkJoinTask 原样提交及失败后
worker 复用；现有 `DispatcherLifecycleTest` 继续验证 lazy 创建、shutdown 与重建。

`PinnedDispatcher` 将 thread-pool 的 core/max pool size 固定为 1，其余 thread-pool 参数保持不变，
并沿用普通 Dispatcher 的 lazy executor、inhabitants 和延迟 shutdown。首次注册绑定 owner；owner
未注销时注册其他 ActorCell 会抛出 `IllegalArgumentException`，注销后允许绑定新 owner。配置的
executor 不是 `ThreadPoolExecutorConfigurator` 时发布与上游一致的 Warning，并回退到
`ThreadPoolConfig` 默认值。`PinnedDispatcherTest` 覆盖独立单线程、别名、owner 生命周期、回退告警
和非法 executor；`ActorRuntimeTest` 覆盖 internal dispatcher 的初始化、guardian 归属、别名，以及
通过 `DispatcherTestSupport` 按有效应用配置创建 Dispatcher。对应上游 `dispatch/PinnedDispatcher.scala`、
`dispatch/Dispatchers.scala`、`dispatch/AbstractDispatcher.scala` 和 `dispatch/ThreadPoolBuilder.scala`。

`ActorSystem.threadFactory()` 返回共享的 `MonitorableThreadFactory`。Scheduler 使用
`<system>-scheduler-<n>`，Dispatcher 使用 `<system>-<dispatcher-id>-<n>`；daemon、上下文类加载器和
uncaught exception handler 保持一致。fatal error 按 `tapio.jvm-exit-on-fatal-error` 决定退出 JVM
或终止 ActorSystem，非 fatal 异常记录到 EventStream。`BlockContext` 已恢复线程私有上下文与
`blockOn` 重投剩余批次的语义；Java 没有 Scala `Batchable` 和 `ControlThrowable`，因此
`ScalaBatchable` 只识别 Tapio `Batchable`，不能识别真正 Scala `Batchable`。ForkJoin worker
仍只保留线程工厂语义，没有 `PekkoManagedBlocker`；fatal 日志的 Security marker 也未移植，
属于已记录的 Java/SLF4J 边界差异。
fork-join 的 `virtualize=on` 会明确拒绝；virtual/affinity executor 和自定义 executor configurator
尚未提供受支持入口。对应上游
`actor/ActorSystem.scala` 的 `threadFactory` 与 `uncaughtExceptionHandler`。

当前支持普通 `Dispatcher` 的 `throughput`、`throughput-deadline-time`、`shutdown-timeout`
（默认 `1s`）和上述内置 executor 配置，以及 `Props.withDispatcher` / `Props.withMailbox` 绑定。
`Deployer` 从 `tapio.actor.deployment` 读取
`dispatcher` 与 `mailbox`，按去掉 guardian 段后的路径匹配；部署配置覆盖 Props，dispatcher 与
mailbox 分别按“部署值 > Props 值 > 默认值”解析，`DispatcherSameAsParent`（`..`）复用 parent 的
dispatcher。对应上游 `actor/Deployer.scala`、`actor/Props.scala` 和
`actor/ActorRefProvider.scala` 的 `LocalActorRefProvider.actorOf`。

`Mailboxes.getMailboxType` 按五层优先级选择 mailbox：部署值、dispatcher 配置的 `mailbox-type`、
Actor 上的 `RequiresMessageQueue` 标记、dispatcher 配置的 `mailbox-requirement`、
`default-mailbox`。dispatcher 的 `mailbox-type` 按 `Dispatchers.config(id).getString("id")` 指向的
配置段查找。`ActorCell.init` 通过 `Dispatcher.createMailbox` 创建 Mailbox，并在把 `Create` 放入
mailbox 前再次校验实际队列是否满足 `RequiresMessageQueue`；校验只在 mailbox type 声明
`ProducesMessageQueue` 时执行，失败时把 `ActorInitializationException` 延迟到 Actor 创建阶段抛出。
对应上游 `dispatch/Mailboxes.scala`、`dispatch/Dispatcher.scala`、`dispatch/Mailbox.scala` 和
`actor/dungeon/Dispatch.scala` 的 `init`。

`UnboundedMailbox` 使用 `ConcurrentLinkedQueue`。`BoundedMailbox` 使用 `LinkedBlockingQueue.offer`，
在 `push-timeout` 内仍无法入队时通过 provider 的 dead letters 发布 `DeadLetter`；
`bounded-capacity:N` 直接按容量创建 `BoundedMailbox`，并复用同一 id 的 mailbox type。
非零 `push-timeout` 由实现 `ProducesPushTimeoutSemanticsMailbox` 的 mailbox type 触发一次告警。
Java `MessageQueue.enqueue` 不能声明 checked `InterruptedException`，因此通过现有泛型重抛机制
让 `offer` 或 `put` 抛出的原异常继续向上传播；不恢复中断标志，也不把当前消息发布为 `DeadLetter`，
与 Pekko 的中断路径一致。只有超过 `push-timeout` 的正常入队失败才发布 `DeadLetter`。
对应上游 `dispatch/Mailbox.scala` 和 `dispatch/Mailboxes.scala`。`WildcardIndex` 保持
`WildcardTree` 的 exact、`*`、`**` 匹配顺序。对应上游
`util/WildcardIndex.scala`。

`DispatcherPrerequisites.dynamicAccess`、`Dispatchers.registerConfigurator` / `from`、
`CachingConfig` 优化、自定义 dispatcher/executor configurator、virtual/affinity executor、
router 分支、其他 mailbox 类型（priority、deque、control-aware、single-consumer）仍待移植。
`configureExecutor` 对未知 executor 名（包括 FQCN）明确抛出 `ConfigurationException`，不会静默回退。
上游 ForkJoin/ThreadPool configurator 内部的 Pool、Task、Factory 和 `LoadMetrics` 类型尚未逐类
对齐；当前保留可观察的池参数、任务包装、拒绝和生命周期语义，不将这部分描述为完整结构 parity。
logger mailbox 已按 `LoggerMessageQueueSemantics` 选择 `LoggerMailbox`，
其终止清理会把满足当前级别的剩余 `LogEvent` 同步刷到 `StandardOutLogger`，其余消息丢弃。
`Dispatcher` 与 `PinnedDispatcher` 的公开构造器恢复上游的 configurator/prerequisites 形状，
不再提供要求 `ActorSystem` 或仅含 throughput 的便捷构造器。测试需要直接创建实例时使用
`DispatcherTestSupport` 按完整参数构造，不在生产 API 中增加上游没有的重载。

## 消息分发

`ActorRef.tell` 创建 `Envelope`，`Dispatcher.dispatch` 将它入队，`Mailbox.run` 优先处理 system
message，再按 throughput 批量处理用户消息。每条普通消息处理完并清理系统消息后，Mailbox 检查批次截止时间。
Mailbox 的 `Open`、`Closed`、`Scheduled` 和 suspend 位
布局与 Pekko 对齐。`BatchingExecutor` 使用 `ThreadLocal<AbstractBatch>` 合并当前线程的嵌套
Batchable 提交；`AbstractBatch` 继承 `ArrayDeque<Runnable>`，普通批次与 `BlockableBatch` 分别恢复
此前的 batch 和 BlockContext。异常时先 `resubmitUnbatched` 重投未执行任务再抛出；`blockOn` 也会
重投剩余批次。非 Batchable 直接 `unbatchedExecute`，virtualized Dispatcher 关闭合并。
`Mailbox` 的 `hasMessages` 和 `numberOfMessages` 只查询普通队列，`hasSystemMessages` 单独检查系统消息。
`Dispatcher.createMailbox` 使用匿名 `DefaultSystemMessageQueue`，Java 用抽象子类承载上游 mixin 的实现。
系统队列和 `ActorCell` 的系统消息暂存都使用 `SystemMessage.next`，以链头表示上游的擦除后的 value class。
系统队列每批排空后继续摘取新到的系统消息，全部处理后传播中断。普通消息处理后若检测到中断，
当前批次立即退出。`TaskInvocation` 实现 `Batchable` 并委托 `ScalaBatchable`，结束或提交失败都释放
inhabitants。`executeTask` 与
`registerForExecution` 连续两次拒绝时发布固定消息 `executeTask was rejected twice!` 或
`registerForExecution was rejected twice!`，Error 的 cause 保留第一次拒绝异常，并抛出第二次异常。
失败报告用线程集合防止 EventStream 重新进入 Dispatcher 时递归；递归分支直接调用
`StandardOutLogger.INSTANCE.tell`，与上游 `StandardOutLogger` 的同步输出路径一致。
`TaskInvocation` 对 null 异常消息使用字符串 `"null"`，因为 Tapio `LogEvent` 要求 message 非空；
上游 Scala 可直接保存 null。shutdown action 通过实现 `ExecutionContext` 的同步 Executor 调度，
其 `reportFailure` 回传当前 MessageDispatcher，避免 shutdown 异常改走线程默认 handler。
对应上游 `dispatch/BatchingExecutor.scala`、`scala.concurrent.BlockContext`、
`dispatch/Dispatcher.scala`、`dispatch/Mailbox.scala`、`dispatch/AbstractMailbox.scala` 和
`dispatch/AbstractDispatcher.scala`。`BatchingExecutorTest` 覆盖嵌套合并、非 Batchable 旁路、
异常重投、blockOn 重投、virtualized 关闭合并与 `LogEventException` 原样发布。

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

`ActorCell.finishTerminate` 保持 `postStop → Dispatcher.detach → parent/watcher 终止通知 →
unwatch → stopped lifecycle → 清理字段` 的顺序。因此 watcher 收到 `Terminated` 时，目标旧
mailbox 已完成关闭和清理，并已切到 dead-letter mailbox。对应上游
`actor/dungeon/FaultHandling.scala` 的 `finishTerminate`。

`ChildRestartStats` 对应 `actor/dungeon/FaultHandling.scala`，其 `uid`、有限重试计数、无限重试、
时间窗累计及过期重置逻辑与上游一致。纳秒时间通过 `System.nanoTime()`，时间窗配置按上游转为毫秒。
`SupervisorStrategy`、`OneForOneStrategy` 和 `AllForOneStrategy` 对齐
`actor/FaultHandling.scala` 中的 `handleFailure`、`processFailure`、`logFailure` 和 `restartChild` 职责。
Java 使用 `Function<Throwable, Directive>` 表达 decider，并以负 `Duration` 表示 Pekko `Duration.Inf`。
未匹配异常返回 `Escalate`，由 `ActorCell.handleFailure` 原样向上抛出；默认策略对
`ActorInitializationException`、`ActorKilledException` 和 `DeathPactException` 执行 Stop，普通
`Exception` 执行 Restart。`stoppingStrategy` 不限制重试次数和时间窗，对 `Exception` 执行 Stop，
其他 `Throwable` 向上升级。

两个具体策略保留上游 Java API：`(int, java.time.Duration, Function, boolean)`、
`(int, java.time.Duration, Function)`、`(int, java.time.Duration, Iterable<Class<? extends Throwable>>)`、
`(boolean, Function)` 和 `(Function)`；不增加上游没有的省略 decider 构造重载。
`makeDecider(JIterable)` 在构造时快照异常类型集合。`OneForOneStrategy.withMaxNrOfRetries`
保留时间窗、decider 和日志设置，仅替换重试上限。

`ActorSystem.Settings` 从 `tapio.actor.guardian-supervisor-strategy` 读取 configurator 全限定类名。
`LocalActorRefProvider` 使用 ActorSystem 的 ClassLoader 反射查找无参构造，调用 `create()` 后将结果
注入 `/user` guardian。`InvocationTargetException` 解包为构造器原异常；Java checked 反射异常沿用
`ActorCell.throwUnchecked` 原样抛出，对应上游 `DynamicAccess.createInstanceFor(...).get` 的传播方式。
`DefaultSupervisorStrategy` 和 `StoppingSupervisorStrategy` 均为 final、无显式构造参数，分别返回
共享的 `defaultStrategy` 与 `stoppingStrategy`。对应上游 `actor/FaultHandling.scala`、
`actor/ActorSystem.scala`、`actor/ActorRefProvider.scala`、`actor/ReflectiveDynamicAccess.scala` 和
`actor/src/main/resources/reference.conf`。测试通过只证明 Tapio 已实现契约可用，不代表通过 Pekko
完整测试套件或达到全量源码 parity。

## 控制消息、ReceiveTimeout 与 restart

`PoisonPill` 和 `Kill` 是 `AutoReceivedMessage`：前者在 auto-receive 路径调用 `self.stop()`，后者抛出
无栈 `ActorKilledException("Kill")` 并进入 supervision。`Kill` 本身不等于 restart；默认策略将其 Stop，
只有 supervisor 选择 Restart 时才重新创建 Actor。`Terminated`、`Identify` 和 `ActorSelectionMessage`
也进入 auto-receive，`tapio.actor.debug.autoreceive` 控制对应 lifecycle 日志。对应上游
`actor/Actor.scala`、`actor/ActorCell.scala`、`actor/Identify.scala`、`actor/ActorSelection.scala` 和
`actor/src/main/resources/reference.conf`。

`ReceiveTimeout` 使用 `timeout/task/version` 三字段状态。`setReceiveTimeout` 只更新时长和版本，
`cancelReceiveTimeout` 将时长设为 undefined；普通消息和 auto 消息在处理前取消 task，处理后按版本决定
是否重排。`NotInfluenceReceiveTimeout` 不取消、不重排，除非 handler 修改了 timeout。`ReceiveTimeout`
自身不属于该 marker，因此会重复触发，sender 为 dead letters。create/preStart 与 restart/postRestart
后强制重排，faultCreate 和 terminate 先取消时长及 task。对应上游 `actor/dungeon/ReceiveTimeout.scala`、
`actor/dungeon/FaultHandling.scala` 和 `actor/AbstractActor.scala`。Java 以 `Duration.ZERO` 表示未设置，
以内部 `null` 表示 undefined。

DeathWatch 使用 `watching: Map[ActorRef, Optional[Any]]` 和 `terminatedQueued` 区分默认
`Terminated` 与 `watchWith` 自定义消息。自定义消息在 `receivedTerminated` 中替换 currentMessage，
再交给当前 behavior；未 watch 或已 unwatch 的 `Terminated` 被丢弃。同一目标混用 `watch`/`watchWith`
或更换自定义消息会抛出 `IllegalStateException`。两张 map 都是 ActorCell 消息线程私有，以 Java
`HashMap` 表达上游 immutable Map 的更新语义。`Optional` 无法表达 `Some(null)`，因此内部用哨兵区分
自定义 null 与默认 `Terminated`，最终仍向 behavior 投递 null。对应上游 `actor/dungeon/DeathWatch.scala`。

`ActorCell.newActor` 先清空旧 behaviorStack，再调用 Props 创建实例；只有 constructor 未调用 `become`
时才压入 `instance.receive()`。因此 constructor 中选定的 behavior 在首次创建和 restart 后都保留。
对应上游 `actor/ActorCell.scala` 的 `newActor`。

constructor/preStart 失败包装为带 ActorRef 的 `ActorInitializationException`，消息前缀为 Actor path；
mailbox 类型的延迟复核失败和 ActorCell 初始化失败也保留该 ActorRef 元数据。`InstantiationException`
保留上游的专门诊断文本，其他异常取最深 root cause 的消息。Creator 的 checked exception 由 Java
`throws Exception` 边界直接传到 ActorCell，只包装一次。preRestart 失败发布 `PreRestartException` Error
后继续 restart；constructor/postRestart 失败设置 fatal，并包装为 `PostRestartException` 交给 supervisor。
`InterruptedException` 在升级给 supervisor 时包装为 `ActorInterruptedException`，处理路径恢复线程
中断标志。对应上游 `actor/Actor.scala`、`actor/ActorCell.scala`、`actor/dungeon/Dispatch.scala`、
`actor/Props.scala`、`actor/IndirectActorProducer.scala` 和 `actor/dungeon/FaultHandling.scala`。

restart 等待 children 期间，system message 按 `Default → Suspended → SuspendedWaitForChildren`
状态处理。Suspended 只暂存 `StashWhenFailed`，等待 children 时只暂存 `StashWhenWaitingForChildren`；
`Failed` 同时实现两者，`Recreate`、`Suspend`、`Resume` 实现后者。每条消息后重新计算状态，只有状态
下降才按原顺序重放暂存消息。`Creation` 和 `Recreation` 实现 `WaitingForChildren`。等待期间保留旧
Actor 实例，postRestart 失败终止时也保留新实例供 `postStop` 调用，与上游生命周期顺序一致。对应上游
`actor/ActorCell.scala` 的 `systemInvoke`、`dispatch/sysmsg/SystemMessage.scala`、
`actor/dungeon/ChildrenContainer.scala` 和 `actor/dungeon/FaultHandling.scala`。

`ClassicControlMessageTest` 以 14 项定向测试覆盖上述契约。测试通过只证明 Tapio 已实现契约可用，
不代表通过 Pekko 完整测试套件或达到全量源码 parity。

## 本地 ask、一次性与周期调度

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
Actor 注销时将系统共享的 dead-letter Mailbox 换入，并关闭、清理旧 Mailbox。
`Mailboxes` 持有全系统唯一的 `DeadLetterMailbox`，所有 Dispatcher 通过注册表委托到同一实例；
该队列不保存消息，普通消息立即转为 `DeadLetter` 投递到 `deadLetters`，已是 `DeadLetter` 的消息不再递归发布。
对应 `actor/ActorRef.scala`、
`actor/ActorRefProvider.scala`、`actor/dungeon/DeathWatch.scala`、`dispatch/AbstractDispatcher.scala`、
`dispatch/Mailboxes.scala` 和 `dispatch/Mailbox.scala`。本地 Actor 和 dead letters 已处理
`Identify` / `ActorIdentity`；引用序列化仍待移植。

`DeadLetterListener` 订阅 `DeadLetter`、`Dropped` 和 `UnhandledMessage`，按上游
`tapio.log-dead-letters`、`tapio.log-dead-letters-suspend-duration` 和
`tapio.log-dead-letters-during-shutdown` 的默认值限制日志。对应
`actor/src/main/scala/org/apache/pekko/event/DeadLetterListener.scala` 和 `actor/ActorSystem.scala`。
系统 Actor 由 `systemActorOf` 挂载到 `/system`。终止时先停止 user guardian，
再由 DeathWatch 依次通知 system guardian 和 root guardian。根的监督锚点收到终止通知后，
Provider 的 `terminationFuture` 以根引用完成，ActorSystem 在 Scheduler 清理后发布这一结果。
对应上游 `actor/ActorRefProvider.scala`、`actor/ActorSystem.scala` 和 `actor/dungeon/FaultHandling.scala`。
系统 logger 通过 `tapio.loggers` 创建。system guardian 的 termination hooks 完成后调用
`stopDefaultLoggers`：恢复 stdout 兜底，逐个先退订再停止 logger，最后才允许 guardian 停止。
ActorSystem 启动失败时不无界等待终止：先停止已创建的 user guardian，仅在其尚未创建时回退到
root guardian，使 `user guardian → system guardian → stopDefaultLoggers` 关闭链保持有效。
日志事件提供 `LogEventWithMarker` 和 `LogMarker`。`LogEvent` 保存不可变的
`mdc()` / `getMDC()` 快照，`LoggingAdapter` 的 `mdc`、`setMDC`、`getMDC` 和 `clearMDC`
在发布时写入事件；stdout 和 SLF4J 均使用该事件 MDC。SLF4J bridge 依次写入 marker 属性、
Pekko 标准字段和事件 MDC，并在日志调用结束后执行 `MDC.clear()`。
`CoordinatedShutdown` 已实现配置阶段 DAG、阶段超时、并行任务、recover、取消句柄和
`ActorSystem.terminate()` 的最后终止任务，并按配置注册可取消的 JVM shutdown hook；
`System.exit`、集群任务和远程 Actor 的扩展仍待移植。

一次性与周期调度移植 `actor/Scheduler.scala`、`actor/LightArrayRevolverScheduler.scala` 和
`dispatch/AbstractNodeQueue.java` 中对应的时间轮、发布队列、取消和关闭路径。fixed-delay
在上次任务完成后重新等待完整间隔；fixed-rate 使用 ideal deadline，每次增加原始 interval，
错过周期时补偿执行且不重叠。initialDelay 非正时直接提交给 Executor，delay/interval 必须严格
大于 0；向上取整后的最大延迟为 `tickNanos * Integer.MAX_VALUE`，包含等号。

周期 handle 使用 `AtomicCancellable`：初始为非 null sentinel，首次任务返回后才发布；null 表示
已取消，`swap` 在取消后取消 replacement，底层 `cancel()` 返回 false 时包装器仍可脱离并返回 true。
ActorRef 重载先 `tell` 再检查 termination，terminated 后静默停止后续调度。普通任务异常会终止
后续调度，但不会自动把 handle 标为 cancelled；fixed-delay 仅吞掉 SchedulerException 及其包装。
`maxFrequency` 返回 `1 second / TickDuration`，默认 10ms tick 对应 100Hz。

关闭时仅运行待执行的 `Scheduler.TaskRunOnClose`。系统关闭时尚未触发的普通 ask 超时任务会丢弃，
对应结果仍可能处于未完成状态。配置位于
`tapio.scheduler`，默认 10ms tick、512 个时间轮槽和 5s 停止等待时间。
普通 Dispatcher 的延迟 shutdown 已由 `TaskRunOnClose` 接入。Actor tree
终止后，ActorSystem 先关闭 Scheduler 并执行 pending Dispatcher shutdown，再完成
`whenTerminated`。Java 使用 `Executor` 对应 Scheduler 的 `ExecutionContext`，一次性调度的
现有无 Executor 重载在触发任务的线程执行。应用通过
`ActorSystem.scheduler()` 取得 Scheduler。自定义实现通过 `tapio.scheduler.implementation`
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
发布通过 `ActorRef.tell` 进入 subscriber mailbox。按 channel 退订会更新该 channel 和已索引的子类型，
保留父类型和无关 channel 的订阅。`SubchannelClassification` 锁定 `SubclassifiedIndex`，
发布仅在 cache miss 时获取同一把锁。Java 将两个含状态的 Scala trait 映射为
`EventStream extends SubchannelClassification extends LoggingBus`，索引和 volatile cache 归前者，
logger 生命周期和日志级别归后者。cache 使用 Java 不可变 Map 和 Set 快照。构造参数为
`ActorSystem` 和可选的 `debug`，默认关闭。对应上游 `event/EventStream.scala`、`event/EventBus.scala`、
`event/Logging.scala` 和 `util/SubclassifiedIndex.scala`。`ActorSystem` 就绪后创建独立的
`EventStreamUnsubscriber` 系统 Actor，使用 internal Dispatcher 和全系统唯一名称。订阅先通过
`AtomicReference` 状态机登记；启动前保存初始订阅集合，启动后切换为 unsubscriber 引用并转发
`Register` / `UnregisterIfNoMoreSubscribedChannels`。unsubscriber 使用 DeathWatch 观察 `InternalActorRef`，
收到 `Terminated` 后取消该订阅者的全部 channel；取消最后一个 channel 时解除观察。对应上游
`event/EventStream.scala`、`event/EventStreamUnsubscriber.scala` 和 `event/EventBus.scala`。
`tapio.actor.debug.event-stream` 默认 `off`，开启后发布与上游文本一致的订阅、退订和观察 DEBUG 事件。
Java 的自定义非 `InternalActorRef` 无法参与本地 DeathWatch，因此 unsubscriber 跳过对这类引用的 watch/unwatch；
普通发布与显式退订仍可使用，但不会获得终止自动退订。

默认 logger 生命周期对应 `startDefaultLoggers` / `stopDefaultLoggers`。启动时先读取
`tapio.stdout-loglevel` 安装同步 `StandardOutLogger`，再从 `tapio.loggers` 解析全部 Actor class；
默认类为 `DefaultLogger`，配置中的 `StandardOutLogger` Actor class 会被排除。所有 class 解析成功后，
按 `"log" + LogExt(system).id() + "-" + simpleName(clazz)` 命名，在 `tapio.loggers-dispatcher` 上创建 logger Actor，
并通过 `InitializeLogger` / `LoggerInitialized` 握手确认初始化，超时为 `tapio.logger-startup-timeout`，
默认 `5s`。`debug.unhandled` 开启时同时创建 `UnhandledMessageForwarder`。

启动结果先保存在本地列表，全部成功后才在锁内发布共享 logger 列表和级别；后续 logger 失败会退订、
停止已启动 logger，并恢复此前状态，清理异常作为 suppressed 保留。启动失败输出
`error while starting up loggers` 和原始堆栈，包装为 `ConfigurationException`；中断会恢复线程中断标志。
关闭时确保 stdout 兜底存在，逐个先 `unsubscribe(logger)` 再 `stop()`，避免停止消息变成 DeadLetter
后再次进入关闭流程。`DefaultLogger` 声明 `LoggerMessageQueueSemantics`；终止前仅把满足当前级别的
剩余日志同步刷到 stdout。对应上游 `event/EventStream.scala`、`event/Logging.scala` 中的 `LoggingBus`、`DefaultLogger`、`StandardOutLogger`，
以及 `event/LoggerMailbox.scala` 和 `event/UnhandledMessageForwarder.scala`。

动态级别使用 `OFF, DEBUG, INFO, WARNING, ERROR` 顺序。`setLogLevel` 按新旧启用级别差异切换订阅，
`OFF` 表示完全关闭；`isEnabled` 同时要求事件级别与当前级别均不为 `OFF`。日志事件记录
`timestamp` 和 `threadName`，stdout 格式保留级别标签、时间、线程、source、marker、MDC、message 和 cause。
`LoggingAdapter` 按上游 `format1` / `formatImpl` 处理模板参数：单参数数组先展开，再逐个替换 `{}`；
普通模板参数中的 `Throwable` 仍作为参数格式化，只有显式 cause 重载才设置事件 cause；
多余参数追加 ` WARNING arguments left: N`。Java varargs 的 null 数组规范化为单参数 `null`，与 Scala 单参数调用一致。

SLF4J bridge 由系统内 logger Actor 消费 `LogEvent`，不创建专用事件分发线程。
`Slf4jLogger` 与 `DefaultLogger` 一样声明 `LoggerMessageQueueSemantics`，使用 `LoggerMailbox`。
标准 MDC 字段为 `sourceThread`、`sourceActorSystem`、`tapioSource`、`tapioTimestamp`、
`tapioAddress` 和 `tapioUid`；marker properties 先写入，再写标准字段，最后写入事件 MDC，
因此同名键由后者覆盖。`tapioUid` 惰性解析，只在非空时写入；MDC 写入无论日志调用是否成功都会清空。
`Slf4jLogger.formatTimestamp` 为 protected 实例扩展点，默认时间格式为 `HH:mm:ss.SSSUTC`；
`StdOutLogger.timestamp` 同样可由子类覆盖。日志参数保持原始 `event.message()`，
不添加 `[source]` 前缀。`Slf4jLogMarker` 保留原始 SLF4J Marker，bridge 优先透传该 Marker，
普通 `LogMarker` 才按名称创建 Marker；初始化握手前通过 SLF4J 输出 `Slf4jLogger started`。
库只依赖 `slf4j-api`，不捆绑 provider；必须显式设置
`tapio.loggers = ["io.github.espen7.tapio.event.slf4j.Slf4jLogger"]` 才会启用。对应上游
`event/Logging.scala` 和 `slf4j/src/main/scala/org/apache/pekko/event/slf4j/Slf4jLogger.scala`。

Java 边界：`Timeout(...)` 日志按上游文本保留，数字由配置毫秒值构造；`UnboundedMailbox.MessageQueue`
改为可继承以容纳 `LoggerMailbox`；`LoggerMailboxType` 公开构造以沿用现有反射式 mailbox type 契约。
`NonFatal` 对应 `scala.util.control.NonFatal`，排除 `VirtualMachineError`、`ThreadDeath`、`LinkageError`
和 `InterruptedException`；Java 没有 Scala `ControlThrowable`。`NoStackTrace` 用 marker interface 对应
Scala trait，stdout 对其异常输出 ` (class: message)` 短文本。logger 清理覆盖非致命 `Error`，
fatal Error 直接抛出。`Settings.Loggers` 从 Config list 复制为不可变 list。
logging filter 和启动时配置日志仍未移植；serialization 不在当前范围内。

## Classic Actor 类型边界

- `ActorRef` 按路径和 incarnation UID 排序，路径按叶节点到根节点比较。Java 的文本构造器仍保留，
  比较从路径文本取得层级；对应 `actor/ActorPath.scala` 和 `actor/ActorRef.scala`。`stop` 和 `isTerminated`
  归 `InternalActorRef`，应用通过 `ActorSystem.stop` 或 `ActorContext.stop` 停止 Actor。
- `RepointableActorRef` 的两个 Cell 引用以 VarHandle 的 volatile 读和 CAS 循环换入并返回先前引用。
  对应 `actor/RepointableActorRef.scala`。Scala `Dispatch` 的带状态 trait 以 Java 抽象基类承载，
  `Children`、`DeathWatch`、`FaultHandling` 和 `ReceiveTimeout` 以接口表达，相关状态和实现保留在
  `ActorCell`。终止由 Mailbox 的 Closed 状态决定，终止清理按上游换入 `terminatedProps`。
  对应 `actor/ActorCell.scala` 和 `actor/dungeon/{Dispatch,Children,DeathWatch,FaultHandling,ReceiveTimeout}.scala`。
- `dispatch/sysmsg/SystemMessage.scala` 的具体消息分别有同名 Java 类。`Create.failure` 用
  `Optional<ActorInitializationException>`，`Watch.watcher` 和 `Unwatch.watcher` 为 `InternalActorRef`。
  `StopChild` 和 `SystemGuardian` 的 termination hook 属于 guardian 的普通消息协议。
  system guardian 在 user guardian 终止后切换 behavior，等待 hooks 完成再停止 logger 和自身。
  对应 `actor/ActorRefProvider.scala`。
- `Actor` 的 Scala trait 有实例字段，因此 Java 将其映射为抽象类。`AbstractActor.Receive` 包装
  `PartialFunction<Object, Void>`，用 Java `Void` 表示仅做副作用的 Scala `Unit`。Java 的
  `applyOrElse` 结果泛型由调用方选择为两个分支的共同类型，对应 Scala 的结果类型下界。
  `AbstractActor.ActorContext` 提供 `Receive` 重载和 Java 访问器，`preRestart` 的消息使用 `Optional<Object>`。
  对应 `actor/Actor.scala`、`actor/AbstractActor.scala` 和 `japi/pf/ReceiveBuilder.java`。

## Java 语言适配

- Scala `object` 入口转换为 Java static method。
- `@tailrec` 转换为循环；Scala `Option`、`Future`、`FiniteDuration` 分别映射为
  `Optional`、`CompletionStage`、`java.time.Duration`。
- Scala `!` 对应公开 `tell` 和内部 `sendMessage`。
- `Props` 通过 Java reflection 选择参数类型匹配的唯一 constructor；基本类型参数先映射到对应 wrapper，
  null 不匹配基本类型。多个匹配 constructor 与无匹配 constructor 都抛出 `IllegalArgumentException`。
- `ActorPath.toStringWithoutAddress` 只返回 path elements，不包含 address。

Tapio 是独立 Java 移植，不复制 Pekko 源文件。Pekko 使用 Apache-2.0 许可证。固定源码位于
[Pekko actor/src/main](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main)。

## Extension、DynamicAccess 与虚拟线程执行器

- `Extension`、`ExtensionId`、`AbstractExtensionId` 和 `ExtensionIdProvider` 对应上游 `actor/Extension.scala`。`ActorSystemImpl` 使用 `ConcurrentHashMap` + `CountDownLatch` 保证同一 `ExtensionId` 在多线程并发注册时只初始化一次，并在构造末尾按 `tapio.library-extensions`（失败即抛出）和 `tapio.extensions`（失败记录日志并跳过）自动加载扩展。
- `DynamicAccess` 和 `ReflectiveDynamicAccess` 对应上游 `actor/DynamicAccess.scala` 和 `actor/ReflectiveDynamicAccess.scala`。Java 版本以 `Class<T>` 参数和 `List<Map.Entry<Class<?>, Object>>` 替代 Scala `ClassTag` 与 `Try`，内部复用 `MethodHandle` 与 `VarHandle` 缓存；`LocalActorRefProvider` 的 `SupervisorStrategyConfigurator` 与 `ActorSystemImpl.createScheduler` 已统一经由 `ExtendedActorSystem.dynamicAccess()` 实例化。
- `VirtualThreadSupport`、`VirtualizedExecutorService` 和 `VirtualThreadExecutorConfigurator` 对应上游 `dispatch/VirtualThreadSupport.scala`、`dispatch/VirtualizedExecutorService.scala` 与 `fork-join-executor` / `thread-pool-executor` 的 `virtualize = on` 及 `executor = "virtual-thread-executor"`。Java 25 上直接使用公开 API `Thread.ofVirtual()` 和 `Executors.newThreadPerTaskExecutor`；绑定自定义底层 `scheduler` 字段时若 JVM 未开启 `--add-opens java.base/java.lang=ALL-UNNAMED`，则自动回退到 JVM 默认虚拟线程调度器。

## Timers、PipeTo、GracefulStop、Stash 与扩展邮箱

- `TimerScheduler`、`TimerSchedulerImpl`、`AbstractActorWithTimers` 和 `AbstractLoggingActorWithTimers` 对应上游 `actor/Timers.scala` 和 `actor/dungeon/TimerSchedulerImpl.scala`。支持 `startSingleTimer`、`startTimerWithFixedDelay`、`startTimerAtFixedRate`、`isTimerActive`、`cancel` 和 `cancelAll`，并在 `aroundPreRestart` / `aroundPostStop` 自动取消；通过 `owner` 引用与 `generation` 令牌丢弃已取消或旧重启实例的过期定时消息。
- `Patterns.pipe` 和 `Patterns.gracefulStop` 对应上游 `pattern/PipeToSupport.scala`、`pattern/GracefulStopSupport.scala` 和 `pattern/Patterns.scala`。`pipe` 支持将 `CompletionStage` 成功值或 `Status.Failure` 转发至 `ActorRef` 或 `ActorSelection`；`gracefulStop` 通过 `PromiseActorRef` 监听目标 `Terminated` 并发送 `PoisonPill` 或自定义停止消息。`ActorSystem.registerOnTermination` 通过原子链式 `CompletableFuture` 节点按后进先出（LIFO）顺序执行终止回调，对齐上游 `TerminationCallbacks`，并在系统已终止后抛出 `RejectedExecutionException`。
- `DequeBasedMessageQueueSemantics`、`UnboundedDequeBasedMailbox`、`BoundedDequeBasedMailbox`、`AbstractActorWithStash`、`AbstractActorWithUnboundedStash`、`AbstractActorWithUnrestrictedStash` 和 `StashOverflowException` 对应上游 `actor/Stash.scala`、`actor/AbstractActor.scala` 和 `dispatch/Mailbox.scala`，并支持 `stash-capacity` 配置与 `preRestart`/`postStop` 自动释放到 `deadLetters`。
- `PriorityGenerator`、`UnboundedPriorityMailbox`、`ControlMessage` 和 `UnboundedControlAwareMailbox` 对应上游 `dispatch/Mailbox.scala` 中的优先级与控制感知邮箱。
- `LoggingReceive` 对应上游 `event/LoggingReceive.scala`，在 `tapio.actor.debug.receive = on` 时发布已处理/未处理消息的 `DEBUG` 日志。

## BackoffSupervisor、CircuitBreaker、FSM 与 LoggingFilter

- `BackoffSupervisor`、`BackoffOpts`、`BackoffOnFailureOptions`、`BackoffOnStopOptions`、`RetrySupport`、`BackoffOnRestartSupervisor` 和 `BackoffOnStopSupervisor` 对应上游 `pattern/BackoffSupervisor.scala`、`pattern/BackoffOptions.scala`、`pattern/HandleBackoff.scala`、`pattern/RetrySupport.scala` 及 `pattern/internal/` 对应实现。Java 将 `HandleBackoff` 提取为抽象基类 `AbstractBackoffSupervisor` 复用状态与协议处理。
- `CircuitBreaker` 和 `CircuitBreakerOpenException` 对应上游 `pattern/CircuitBreaker.scala`，保留 `Closed` / `Open` / `HalfOpen` 原子状态机、`callWithCircuitBreaker`、`callWithSyncCircuitBreaker` 与状态转换监听器。
- `FSM`、`AbstractFSM`、`FSMStateFunctionBuilder` 和 `FSMTransitionHandlerBuilder` 对应上游 `actor/FSM.scala`、`actor/AbstractFSM.scala` 与 `japi/pf/FSM*.scala`，支持状态超时（`FSM.StateTimeout`）、`onTransition`、`onTermination` 与 `whenUnhandled`。
- `LoggingFilter`、`DefaultLoggingFilter`、`tapio.logging-filter` 和 `tapio.log-config-on-start`（以及 `ActorSystem.logConfiguration()`）对应上游 `event/Logging.scala` 与 `actor/ActorSystem.scala`。
- `AbstractLoggingActorWithStash` 和 `AbstractActorWithUnrestrictedStash.unstashAll(Predicate<Object>)` 补齐了 `Stash` 体系的日志组合与过滤重放能力。
- `PriorityQueueStabilizer`、`StablePriorityQueue` 和 `StablePriorityBlockingQueue` 对应上游 `util/StablePriorityQueue.scala`；`UnboundedPriorityMailbox.MessageQueue` 继承 `StablePriorityBlockingQueue<Envelope>`，保证相同优先级消息按 FIFO 顺序出队。
- `NodeMessageQueue` 和 `SingleConsumerOnlyUnboundedMailbox` 对应上游 `dispatch/Mailbox.scala`，复用 `AbstractNodeQueue<Envelope>` 实现多生产者单消费者无界邮箱。
- `RetrySupport.retry` 对应上游 `pattern/RetrySupport.scala`，支持立即重试、固定延迟、指数退避及自定义重试条件与延迟函数。
- `Dispatchers.registerConfigurator` 对应上游 `dispatch/Dispatchers.scala`；`Setup` 与 `ActorSystemSetup` 对应上游 `actor/setup/ActorSystemSetup.scala`。

## 源码对齐与 Java 边界记录

- **已对齐项**：
  - `Slf4jLogger` 与 `Slf4jLogMarker` 已移至 `io.github.espen7.tapio.event.slf4j`（对应 `org.apache.pekko.event.slf4j`）；`TimerSchedulerImpl` 保留在 `actor.dungeon`（与 Pekko `actor/dungeon/TimerSchedulerImpl.scala` 一致）。
  - 补齐 `MultipleConsumerSemantics`、`QueueBasedMessageQueue`、`UnboundedQueueBasedMessageQueue`、`BoundedQueueBasedMessageQueue`、`DequeBasedMessageQueue` 队列语义层级。
  - 补齐 `@DoNotInherit`、`@InternalApi`、`@InternalStableApi` 注解与 `NoSerializationVerificationNeeded`、`NoStackTrace`（含 `fillInStackTrace()` 返回 `this`）契约。
  - `DeadLetterMailbox` 已内聚为 `Mailboxes` 内部创建逻辑；移除非上游的 `ActorRef.tell(Object)` 1 参数重载、`ActorSystem.INTERNAL_DISPATCHER_ID`、`ActorCell.cancelReceiveTimeout()` 覆写及 `BackoffOnFailureOptions.withDefaultStoppingStrategy()`。
  - `AbstractNodeQueue` 补齐 `peekNode()` / `peek()` 及 `Node` 的公开构造与访问器；`LoggingReceive.create` 对齐 `(Receive, ActorContext)` 参数顺序并按 `EventStream.logLevel()` 判定。
  - `Props` 补齐 `Props.empty` 与 `Serializable`；`Deploy` 实现 `Serializable`；`DeadLetterListener` 改为无参构造从 `Settings` 读取配置，并补齐 `CoordinatedShutdown` 期间（`!LogDeadLettersDuringShutdown` 且 `terminatingShutdownInProgress`）的死信日志抑制与 `receiveWhenSuspended` 计数偏移（`count - maxCount - 1`）；`AbstractFSM` 补齐 `makeTransition` 目标状态存在性校验与 `logTermination`。
  - `ActorPath` 拆分为 `@DoNotInherit` 密封接口 `ActorPath`、`RootActorPath` 与 `ChildActorPath`（含 `fromString`、`isValidPathElement`、`toSerializationFormat` 与 MurmurHash `hashCode`）；`ActorSystemImpl` 分离 `start()` 启动阶段；`Dispatchers` 补齐 `from(Config)` 与基于 `DynamicAccess` 的 FQCN 动态实例化；新增 `LoadMetrics` 并接入 `ForkJoinExecutorConfigurator`、`ThreadPoolExecutorConfigurator` 与 `VirtualizedExecutorService`。
  - `Props.validate` 补齐抽象类与非 `Actor` 类型的构造期即时校验；`ActorSystemImpl` 与 `LocalActorRefProvider` 补齐内部 `guardianProps` 支持；`Scheduler` 与 `MessageDispatcher` 移除非上游的 `AutoCloseable` 继承（仅 `LightArrayRevolverScheduler` 实现 `java.io.Closeable`）。
- **Java 边界保留适配**：
  - `dispatch/{BlockContext,CanAwait,ExecutionContext}` 与 `util/{NonFatal,NoStackTrace}` 为 Scala 标准库接口/工具在 Java 侧的最小等价替代。
  - `SubchannelClassification extends LoggingBus`：Java 不支持多继承且 `SubchannelClassification` 持有订阅索引状态，故以抽象类单继承承载（上游为 `EventStream extends LoggingBus with SubchannelClassification`）。
  - `ActorCell.makeChild`：`LocalActorRefProvider.actorOf` 不声明受检 `InterruptedException`，故在 `catch (Throwable)` 中以 `instanceof InterruptedException` 分支对齐上游处理。
- **范围外/未移植项**：`Deploy` 其余远端/router 字段、Java 序列化 `writeReplace()`、Remote/Cluster/Sharding/Typed/Routing/I/O 均不在首版范围内。
