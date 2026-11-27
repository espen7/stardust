/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.actor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.setup.ActorSystemSetup;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.MonitorableThreadFactory;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.event.LoggingFilter;
import io.github.espen7.tapio.event.DeadLetterListener;
import io.github.espen7.tapio.util.NonFatal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ActorSystem 的本地实现。
 *
 * <p>先创建 Settings、EventStream、Log 和 Scheduler，再创建 Dispatchers、Mailboxes 与
 * 默认 Dispatcher。Provider 在全部 runtime 组件就绪后启动 root guardian，并在其下注册
 * user guardian 和 system guardian。关闭时从 user guardian 开始。</p>
 */
@InternalApi
public class ActorSystemImpl extends ExtendedActorSystem {

    private static final String ACTOR_NAME_PATTERN = "^[a-zA-Z0-9][a-zA-Z0-9-_]*$";

    /** ActorSystem 的逻辑名称，同时用于生成 ActorPath 和线程名称。 */
    private final String name;
    /** 保存合并后的系统配置和值已解析的 runtime 设置。 */
    private final Settings settings;
    /** 用于加载与实例化可配置组件的反射访问入口。 */
    private final DynamicAccess dynamicAccess;
    /** 已注册的扩展实例（初始化期间暂存 CountDownLatch，失败时暂存 Throwable）。 */
    private final ConcurrentHashMap<ExtensionId<?>, Object> extensions = new ConcurrentHashMap<>();
    /** 根据配置创建并管理此系统的 Dispatcher。 */
    private final Dispatchers dispatchers;
    /** 按配置、Props 和 requirement 选择 mailbox type 的注册表。 */
    private final Mailboxes mailboxes;
    /** 负责提交 Actor mailbox 任务的共享 Dispatcher。 */
    private final MessageDispatcher dispatcher;
    /** 负责一次性调度和关闭阶段定时任务的共享 Scheduler。 */
    private final Scheduler scheduler;
    /** Scheduler 和 Dispatcher 共用的基础线程工厂。 */
    private final MonitorableThreadFactory threadFactory;
    /** 当前系统的 EventStream，承载日志和 dead-letter 事件。 */
    private final EventStream eventStream;
    /** 绑定系统 source 的 LoggingAdapter。 */
    private final LoggingAdapter log;
    /** 当前系统的日志过滤器。 */
    private final LoggingFilter logFilter;
    /** 创建本地 ActorRef、guardian 和临时引用的 Provider。 */
    private final LocalActorRefProvider provider;
    /** 系统级 dead-letter listener；关闭期间可按配置保留或停止。 */
    private final ActorRef logDeadLetterListener;
    /** 管理阶段化关闭任务和 JVM shutdown hook。 */
    private final CoordinatedShutdown coordinatedShutdown;
    /** ActorSystem 完成终止时完成的通知。 */
    private final CompletableFuture<Terminated> whenTerminated = new CompletableFuture<>();
    /** 按后进先出（LIFO）顺序串联终止回调的头指针；为 null 表示系统已终止。 */
    private final java.util.concurrent.atomic.AtomicReference<CompletableFuture<Terminated>> terminationCallbacksRef =
            new java.util.concurrent.atomic.AtomicReference<>(whenTerminated);

    /**
     * 创建本地 ActorSystem。
     *
     * @param name system name
     * @throws IllegalArgumentException 当名称不符合 Pekko 命名规则时
     */
    public ActorSystemImpl(String name) {
        this(name, null, ActorSystemSetup.empty());
    }

    ActorSystemImpl(String name, Config applicationConfig) {
        this(name, applicationConfig, ActorSystemSetup.empty());
    }

    ActorSystemImpl(String name, Config applicationConfig, ActorSystemSetup setup) {
        if (name == null || !name.matches(ACTOR_NAME_PATTERN)) {
            throw new IllegalArgumentException(
                    "invalid ActorSystem name [" + name
                            + "], must contain only word characters (i.e. [a-zA-Z0-9] plus non-leading '-' or '_')");
        }
        this.name = name;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) classLoader = ActorSystem.class.getClassLoader();
        Config config = applicationConfig == null ? ConfigFactory.load(classLoader)
                : applicationConfig.withFallback(ConfigFactory.defaultReferenceUnresolved(classLoader)).resolve();
        this.settings = new Settings(classLoader, config, name, setup);
        this.dynamicAccess = createDynamicAccess();
        this.eventStream = new EventStream(this, settings.DebugEventStream);
        eventStream.startStdoutLogger(settings);
        this.log = Logging.getLogger(eventStream, ActorPath.Scheme + "://" + name, getClass());
        this.logFilter = createLoggingFilter();
        this.threadFactory = new MonitorableThreadFactory(name, settings.Daemonicity, classLoader,
                uncaughtExceptionHandler());
        MessageDispatcher startedDispatcher = null;
        Scheduler startedScheduler = null;
        CoordinatedShutdown startedShutdown = null;
        LocalActorRefProvider startedProvider = null;
        try {
            this.scheduler = startedScheduler = createScheduler();
            this.provider = startedProvider = new LocalActorRefProvider(this);
            this.mailboxes = new Mailboxes(settings, eventStream, provider.deadLetters());
            this.dispatchers = new Dispatchers(settings, this);
            this.dispatcher = startedDispatcher = dispatchers.defaultGlobalDispatcher();
            this.coordinatedShutdown = startedShutdown = new CoordinatedShutdown(this);
            provider.terminationFuture().whenComplete(this::finishTermination);
            provider.init(this);
            if (settings.LogDeadLetters > 0) {
                this.logDeadLetterListener = systemActorOf(Props.create(DeadLetterListener.class), "deadLetterListener");
            } else {
                this.logDeadLetterListener = null;
            }
            eventStream.startUnsubscriber();
            loadExtensions();
            if (settings.LogConfigOnStart) {
                logConfiguration();
            }
            if (settings.CoordinatedShutdownTerminateActorSystem) {
                coordinatedShutdown.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "terminate-system", () -> {
                    finalTerminate();
                    return whenTerminated.thenApply(ignored -> Done.done());
                });
            }
        } catch (RuntimeException | Error error) {
            if (error instanceof Error && !NonFatal.apply(error)) throw error;
            if (startedShutdown != null) startedShutdown.cancelJvmShutdownHook();
            boolean terminationInitiated = false;
            if (startedProvider != null) {
                try {
                    LocalActorRef guardian = startedProvider.guardian();
                    if (guardian == null) guardian = startedProvider.rootGuardian();
                    if (guardian != null) {
                        guardian.stop();
                        terminationInitiated = true;
                    }
                } catch (RuntimeException | Error closeError) {
                    if (NonFatal.apply(closeError)) error.addSuppressed(closeError);
                    else throw closeError;
                }
            }
            if (!terminationInitiated) {
                try {
                    if (startedScheduler != null) startedScheduler.close();
                } catch (RuntimeException | Error closeError) {
                    if (NonFatal.apply(closeError)) error.addSuppressed(closeError);
                    else throw closeError;
                }
                try {
                    if (startedDispatcher != null) startedDispatcher.close();
                } catch (RuntimeException | Error closeError) {
                    if (NonFatal.apply(closeError)) error.addSuppressed(closeError);
                    else throw closeError;
                }
            }
            throw error;
        }
    }

    protected Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
        return (thread, cause) -> {
            if (cause instanceof InterruptedException || NonFatal.apply(cause)) {
                log.error(cause, "Uncaught error from thread [{}]", thread.getName());
            } else if (settings.JvmExitOnFatalError) {
                try {
                    logFatalError("shutting down JVM since 'tapio.jvm-exit-on-fatal-error' is enabled for", cause, thread);
                } finally {
                    System.exit(-1);
                }
            } else {
                try {
                    logFatalError("shutting down", cause, thread);
                } finally {
                    terminate();
                }
            }
        };
    }

    private void logFatalError(String message, Throwable cause, Thread thread) {
        System.err.print("Uncaught error from thread [" + thread.getName() + "]: ");
        System.err.print(cause.getMessage() + ", " + message + " ActorSystem[" + name + "]");
        System.err.println();
        System.err.flush();
        cause.printStackTrace(System.err);
        System.err.flush();
        log.error(cause, "Uncaught error from thread [{}]: " + cause.getMessage() + ", " + message + " ActorSystem[{}]",
                thread.getName(), name);
    }

    protected DynamicAccess createDynamicAccess() {
        return new ReflectiveDynamicAccess(settings.classLoader());
    }

    @Override
    public DynamicAccess dynamicAccess() {
        return dynamicAccess;
    }

    /**
     * 按 {@code tapio.scheduler.implementation} 创建并启动系统调度器。
     *
     * @return 为当前系统创建的调度器
     * @throws IllegalArgumentException 无法加载或构造所配置的调度器
     */
    protected Scheduler createScheduler() {
        String className = settings.SchedulerClass;
        ThreadFactory schedulerThreadFactory = threadFactory.withName(threadFactory.name() + "-scheduler");
        try {
            return dynamicAccess.createInstanceFor(className, Scheduler.class, List.of(
                    Map.entry(Config.class, settings.config()),
                    Map.entry(LoggingAdapter.class, log),
                    Map.entry(ThreadFactory.class, schedulerThreadFactory)));
        } catch (Throwable error) {
            if (error instanceof RuntimeException runtimeError) throw runtimeError;
            if (error instanceof Error fatal) throw fatal;
            throw new IllegalArgumentException("Could not instantiate Scheduler class [" + className + "]", error);
        }
    }

    /**
     * 返回 system name。
     *
     * @return system name
     */
    public String name() {
        return name;
    }

    /**
     * 返回本地 ActorRefProvider。
     *
     * @return 当前系统的 Provider
     */
    public LocalActorRefProvider provider() {
        return provider;
    }

    /**
     * INTERNAL API：返回本地 ActorSystem 的 UID。
     *
     * @return 系统 UID
     */
    @InternalApi
    public long uid() {
        try {
            return provider.systemUid();
        } catch (RuntimeException | Error error) {
            if (!NonFatal.apply(error)) throw error;
            throw new IllegalStateException(
                    "uid accessed before provider has been initialized. "
                            + "This is a bug, please report at https://github.com/apache/pekko/issues",
                    error);
        }
    }

    @Override
    public ActorRef actorOf(Props props) {
        return provider.guardian().underlying().attachChild(props, false);
    }

    @Override
    public ActorRef actorOf(Props props, String name) {
        return provider.guardian().underlying().attachChild(props, name, false);
    }

    @Override
    public ActorRef systemActorOf(Props props, String name) {
        return provider.systemGuardian().underlying().attachChild(props, name, true);
    }

    @Override
    public ActorSelection actorSelection(String path) {
        return ActorSelection.apply(provider, provider.rootGuardian(), path);
    }

    @Override
    public ActorSelection actorSelection(ActorPath path) {
        return ActorSelection.apply(provider, path);
    }

    @Override
    public void stop(ActorRef actor) {
        ActorPath parent = actor.path().parent();
        LocalActorRef guardian = provider.guardian();
        LocalActorRef systemGuardian = provider.systemGuardian();
        if (guardian.path().equals(parent)) {
            guardian.tell(new StopChild(actor), ActorRef.noSender);
        } else if (systemGuardian.path().equals(parent)) {
            systemGuardian.tell(new StopChild(actor), ActorRef.noSender);
        } else {
            ((InternalActorRef) actor).stop();
        }
    }

    @Override
    public Settings settings() {
        return settings;
    }

    /** 返回此 ActorSystem 的 CoordinatedShutdown。 */
    public CoordinatedShutdown coordinatedShutdown() {
        return coordinatedShutdown;
    }

    @Override
    public ActorRef deadLetters() {
        return provider.deadLetters();
    }

    @Override
    public MessageDispatcher dispatcher() {
        return dispatcher;
    }

    @Override
    public Dispatchers dispatchers() {
        return dispatchers;
    }

    /** 返回此系统的 mailbox 注册表。 */
    public Mailboxes mailboxes() {
        return mailboxes;
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public ThreadFactory threadFactory() {
        return threadFactory;
    }

    @Override
    public EventStream eventStream() {
        return eventStream;
    }

    @Override
    public LoggingAdapter log() {
        return log;
    }

    @Override
    public LoggingFilter logFilter() {
        return logFilter;
    }

    @Override
    public void logConfiguration() {
        log.info(settings.config().root().render());
    }

    private LoggingFilter createLoggingFilter() {
        try {
            return dynamicAccess.createInstanceFor(settings.LoggingFilter, LoggingFilter.class, List.of(
                    Map.entry(Settings.class, settings),
                    Map.entry(EventStream.class, eventStream)));
        } catch (Throwable error) {
            if (error instanceof RuntimeException runtimeError) throw runtimeError;
            if (error instanceof Error fatal) throw fatal;
            throw new IllegalArgumentException("Could not instantiate LoggingFilter [" + settings.LoggingFilter + "]", error);
        }
    }

    boolean debugLifecycle() {
        return settings.DebugLifecycle;
    }

    boolean debugAutoReceive() {
        return settings.DebugAutoReceive;
    }

    boolean debugUnhandled() {
        return settings.DebugUnhandledMessage;
    }

    /**
     * 发起系统关闭。
     *
     * <p>Terminate 是幂等的。Actor tree 先结束，Scheduler 和 Dispatcher 随后
     * 释放，最后完成 whenTerminated。</p>
     */
    @Override
    public CompletionStage<Terminated> terminate() {
        if (settings.CoordinatedShutdownRunByActorSystemTerminate) {
            coordinatedShutdown.runAll(CoordinatedShutdown.actorSystemTerminateReason());
        } else {
            finalTerminate();
        }
        return whenTerminated;
    }

    @SuppressWarnings("unchecked")
    private <T extends Extension> T findExtension(ExtensionId<T> ext) {
        Object value = extensions.get(ext);
        if (value instanceof CountDownLatch latch) {
            boolean interrupted = false;
            try {
                long timeout = settings.config().getDuration(ActorPath.Scheme + ".actor.creation-timeout").toNanos();
                while (true) {
                    try {
                        if (!latch.await(timeout, TimeUnit.NANOSECONDS)) {
                            throw new IllegalStateException(
                                    "Extension [" + ext + "] is still being created after the creation-timeout");
                        }
                        break;
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
            return findExtension(ext);
        }
        if (value instanceof Throwable error) {
            ActorCell.<RuntimeException>throwUnchecked(error);
            return null;
        }
        return (T) value;
    }

    @Override
    public final <T extends Extension> T registerExtension(ExtensionId<T> ext) {
        while (true) {
            T existing = findExtension(ext);
            if (existing != null) {
                return existing;
            }
            CountDownLatch inProcessOfRegistration = new CountDownLatch(1);
            if (extensions.putIfAbsent(ext, inProcessOfRegistration) == null) {
                try {
                    T instance = ext.createExtension(this);
                    if (instance == null) {
                        throw new IllegalStateException("Extension instance created as 'null' for extension [" + ext + "]");
                    }
                    extensions.replace(ext, inProcessOfRegistration, instance);
                    return instance;
                } catch (Throwable error) {
                    extensions.replace(ext, inProcessOfRegistration, error);
                    ActorCell.<RuntimeException>throwUnchecked(error);
                    return null;
                } finally {
                    inProcessOfRegistration.countDown();
                }
            }
        }
    }

    @Override
    public <T extends Extension> T extension(ExtensionId<T> ext) {
        T found = findExtension(ext);
        if (found == null) {
            throw new IllegalArgumentException("Trying to get non-registered extension [" + ext + "]");
        }
        return found;
    }

    @Override
    public boolean hasExtension(ExtensionId<? extends Extension> ext) {
        return findExtension(ext) != null;
    }

    private void loadExtensions() {
        loadExtensions(ActorPath.Scheme + ".library-extensions", true);
        loadExtensions(ActorPath.Scheme + ".extensions", false);
    }

    private void loadExtensions(String key, boolean throwOnLoadFail) {
        for (String fqcn : settings.config().getStringList(key)) {
            Object loaded;
            try {
                try {
                    loaded = dynamicAccess.getObjectFor(fqcn, Object.class);
                } catch (Throwable firstProblem) {
                    try {
                        loaded = dynamicAccess.createInstanceFor(fqcn, Object.class, List.of());
                    } catch (Throwable ignored) {
                        throw firstProblem;
                    }
                }
            } catch (Throwable problem) {
                if (!throwOnLoadFail) {
                    log.error(problem, "While trying to load extension [{}], skipping...", fqcn);
                    continue;
                }
                throw new RuntimeException("While trying to load extension [" + fqcn + "]", problem);
            }
            if (loaded instanceof ExtensionIdProvider providerExt) {
                registerExtension(providerExt.lookup());
            } else if (loaded instanceof ExtensionId<?> id) {
                registerExtension(id);
            } else {
                if (!throwOnLoadFail) {
                    log.error("[{}] is not an 'ExtensionIdProvider' or 'ExtensionId', skipping...", fqcn);
                } else {
                    throw new RuntimeException("[" + fqcn + "] is not an 'ExtensionIdProvider' or 'ExtensionId'");
                }
            }
        }
    }

    @Override
    public CompletionStage<Terminated> getWhenTerminated() {
        return whenTerminated;
    }

    @Override
    public void registerOnTermination(Runnable code) {
        java.util.Objects.requireNonNull(code, "code must not be null");
        for (; ; ) {
            CompletableFuture<Terminated> current = terminationCallbacksRef.get();
            if (current == null) {
                throw new java.util.concurrent.RejectedExecutionException("ActorSystem already terminated.");
            }
            CompletableFuture<Terminated> next = new CompletableFuture<>();
            if (terminationCallbacksRef.compareAndSet(current, next)) {
                next.whenComplete((terminated, error) -> {
                    try {
                        code.run();
                    } finally {
                        if (error != null) current.completeExceptionally(error);
                        else current.complete(terminated);
                    }
                });
                return;
            }
        }
    }

    private void finishTermination(Terminated terminated, Throwable failure) {
        try {
            scheduler.close();
        } catch (RuntimeException error) {
            log.error(error, "Error during actor system termination");
        }
        CompletableFuture<Terminated> head = terminationCallbacksRef.getAndSet(null);
        if (head != null) {
            if (failure == null) head.complete(terminated);
            else head.completeExceptionally(failure);
        }
    }

    void finalTerminate() {
        if (!settings.LogDeadLettersDuringShutdown && logDeadLetterListener != null) {
            stop(logDeadLetterListener);
        }
        provider.guardian().stop();
        coordinatedShutdown.cancelJvmShutdownHook();
    }

    /** 按 Pekko close 超时等待 ActorSystem 完成终止。 */
    @Override
    public void close() throws TimeoutException {
        terminate();
        try {
            whenTerminated.get(settings.config().getDuration(ActorPath.Scheme + ".coordinated-shutdown.close-actor-system-timeout")
                    .toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while closing ActorSystem", error);
        } catch (ExecutionException error) {
            throw new IllegalStateException("ActorSystem did not terminate", error);
        }
    }
}
