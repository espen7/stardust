# Actor 源码移植记录

移植对照基线为 Pekko commit `85ab376242152046740a63307304a2e062990f67`。Java 包前缀为
`io.github.espen7.tapio`，相对包路径、class 名称和 method 名称尽量与 Pekko 一致。
`ActorPath.Scheme` 统一提供 `tapio` ActorPath scheme 和 runtime 配置根，避免在 runtime 类中重复硬编码。
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
对应上游 `actor/ActorPath.scala`、`actor/ActorRef.scala`、`actor/dungeon/Children.scala`
和 `actor/dungeon/Dispatch.scala`。guardian 复用现有默认 Dispatcher。独立 internal dispatcher、
额外顶级名称注册和 Remote 仍待移植。

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
`Settings/EventStream/Log → Scheduler → Dispatchers → Mailboxes → 默认 Dispatcher →
CoordinatedShutdown → Provider` 的顺序启动，确保 Dispatcher 创建时 Scheduler 已可用。
关闭任务配置要求 `run-by-actor-system-terminate=on` 时同时启用 `terminate-actor-system`。对应上游
`actor/src/main/scala/org/apache/pekko/actor/ActorSystem.scala` 的 `Settings` 和 `ActorSystemImpl`，
以及 `actor/src/main/scala/org/apache/pekko/PekkoException.scala` 的 `ConfigurationException`。
Java 版本将项目自身的异常基类命名为 `TapioException`；上游 `PekkoException.scala` 仅作为对照来源保留。
Java 版本保留本地 runtime 已消费的 Settings 字段。`tapio.actor.loglevel` 通过
`reference.conf` 提供 `tapio.loglevel` 的回退值。有限的 dead-letter 暂停配置按上游转换为毫秒，
无限期以 `null` 表示。日志沿用原配置文本，Pekko 使用 Scala Duration 的 `pretty` 格式。

系统 Dispatcher 通过 `Dispatchers.defaultGlobalDispatcher`、`lookup` 和
`DispatcherConfigurator.dispatcher` 创建。命名配置继承默认配置段，别名解析以 20 层为限。
按上游用 `ConcurrentHashMap.putIfAbsent` 缓存 configurator；竞争落败的 lazy 实例没有创建
executor，不再额外关闭。注册表不在 ActorSystem 终止时批量关闭实例，实例由各自的 idle
shutdown action 释放。对应上游 `actor/src/main/scala/org/apache/pekko/dispatch/Dispatchers.scala`、
`dispatch/Dispatcher.scala`、`dispatch/AbstractDispatcher.scala` 和
`actor/src/main/resources/reference.conf`。

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
`thread-pool-executor`。默认 executor 通过同一配置段内的 `default-executor.fallback` 回退；
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
池大小、队列、拒绝策略、线程工厂、JDK 25 minimum-runnable 和非法配置；现有
`DispatcherLifecycleTest` 继续验证 lazy 创建、shutdown 与重建。

`ActorSystem.threadFactory()` 返回共享的 `MonitorableThreadFactory`。Scheduler 使用
`<system>-scheduler-<n>`，Dispatcher 使用 `<system>-<dispatcher-id>-<n>`；daemon、上下文类加载器和
uncaught exception handler 保持一致。fatal error 按 `tapio.jvm-exit-on-fatal-error` 决定退出 JVM
或终止 ActorSystem，非 fatal 异常记录到 EventStream。Java 没有 Scala `BlockContext`，因此
ForkJoin worker 只保留线程工厂语义，没有 `PekkoManagedBlocker`；Scala `ControlThrowable` 和
fatal 日志的 Security marker 也未移植，属于已记录的 Java/SLF4J 边界差异。
`virtualize=on`、virtual/affinity executor、自定义 executor configurator 和外部默认
`ExecutionContext` 当前会明确拒绝或尚未提供入口，不会静默忽略。对应上游
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
Java `MessageQueue.enqueue` 不能声明 checked `InterruptedException`，因此有界队列在 `offer` 或 `put`
被中断时恢复线程中断标志并发布当前消息的 `DeadLetter`；Pekko 的对应实现会让该异常继续传播，
这是 Java 接口边界造成的已知可观察差异。
对应上游 `dispatch/Mailbox.scala` 和 `dispatch/Mailboxes.scala`。`WildcardIndex` 保持
`WildcardTree` 的 exact、`*`、`**` 匹配顺序。对应上游
`util/WildcardIndex.scala`。

独立的 internal dispatcher、PinnedDispatcher、自定义 dispatcher/executor configurator、
virtual/affinity executor、外部默认 `ExecutionContext`、`CachingConfig` 优化、router 分支、
其他 mailbox 类型（priority、deque、control-aware、single-consumer）和 logger mailbox 仍待移植。
Java 公开 Dispatcher 构造器要求 `ActorSystem`，
不再提供没有 Scheduler 的无参或仅含 throughput 的假生命周期入口。

## 消息分发

`ActorRef.tell` 创建 `Envelope`，`Dispatcher.dispatch` 将它入队，`Mailbox.run` 优先处理 system
message，再按 throughput 批量处理用户消息。每条普通消息处理完并清理系统消息后，Mailbox 检查批次截止时间。
Mailbox 的 `Open`、`Closed`、`Scheduled` 和 suspend 位
布局与 Pekko 对齐。普通 `Dispatcher.execute(Runnable)` 当前只走 unbatched `TaskInvocation`，
没有移植 batching；task 结束或提交失败都会释放 inhabitants。`executeTask` 与
`registerForExecution` 连续两次拒绝时发布固定消息 `executeTask was rejected twice!` 或
`registerForExecution was rejected twice!`，Error 的 cause 保留第一次拒绝异常，并抛出第二次异常。
失败报告用线程集合防止 EventStream 重新进入 Dispatcher 时递归；Tapio 尚无
`StandardOutLogger`，递归分支改用 `slf4j-api` 记录，这是一处已记录的适配差异。
对应上游 `dispatch/Dispatcher.scala`、`dispatch/Mailbox.scala`、`dispatch/AbstractMailbox.scala`
和 `dispatch/AbstractDispatcher.scala`。

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
Actor 注销时将该 Dispatcher 持有的 dead-letter Mailbox 换入，并关闭、清理旧 Mailbox。
当前每个 Dispatcher 各自持有 dead-letter mailbox；上游 `Mailboxes` 共享实例的拓扑尚未对齐。对应 `actor/ActorRef.scala`、
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
系统 logger 仍作为 system guardian 的子 Actor 随它停止，完整的 `stopDefaultLoggers` 生命周期尚未移植。
日志事件提供
`LogEventWithMarker` 和 `LogMarker`，SLF4J bridge 会将 marker 属性写入 MDC 后清理。
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

Tapio 是独立 Java 移植，不复制 Pekko 源文件。Pekko 使用 Apache-2.0 许可证。固定源码位于
[Pekko actor/src/main](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main)。
