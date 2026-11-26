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
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.MonitorableThreadFactory;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.event.DeadLetterListener;
import io.github.espen7.tapio.util.NonFatal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
    /** 创建本地 ActorRef、guardian 和临时引用的 Provider。 */
    private final LocalActorRefProvider provider;
    /** 系统级 dead-letter listener；关闭期间可按配置保留或停止。 */
    private final ActorRef logDeadLetterListener;
    /** 管理阶段化关闭任务和 JVM shutdown hook。 */
    private final CoordinatedShutdown coordinatedShutdown;
    /** ActorSystem 完成终止时完成的通知。 */
    private final CompletableFuture<Terminated> whenTerminated = new CompletableFuture<>();

    /**
     * 创建本地 ActorSystem。
     *
     * @param name system name
     * @throws IllegalArgumentException 当名称不符合 Pekko 命名规则时
     */
    public ActorSystemImpl(String name) {
        this(name, null);
    }

    ActorSystemImpl(String name, Config applicationConfig) {
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
        this.settings = new Settings(classLoader, config, name);
        this.eventStream = new EventStream(this, settings.DebugEventStream);
        eventStream.startStdoutLogger(settings);
        this.log = Logging.getLogger(eventStream, ActorPath.Scheme + "://" + name, getClass());
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
                this.logDeadLetterListener = systemActorOf(Props.create(DeadLetterListener.class,
                        settings.LogDeadLetters, settings.LogDeadLettersSuspendDuration,
                        settings.logDeadLettersSuspendDurationDescription), "deadLetterListener");
            } else {
                this.logDeadLetterListener = null;
            }
            eventStream.startUnsubscriber();
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
            return Class.forName(className).asSubclass(Scheduler.class)
                    .getConstructor(Config.class, LoggingAdapter.class, ThreadFactory.class)
                    .newInstance(settings.config(), log, schedulerThreadFactory);
        } catch (java.lang.reflect.InvocationTargetException error) {
            if (error.getCause() instanceof RuntimeException runtimeError) throw runtimeError;
            if (error.getCause() instanceof Error fatal) throw fatal;
            throw new IllegalArgumentException("Could not instantiate Scheduler class [" + className + "]", error.getCause());
        } catch (ReflectiveOperationException | ClassCastException error) {
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
            guardian.tell(new StopChild(actor));
        } else if (systemGuardian.path().equals(parent)) {
            systemGuardian.tell(new StopChild(actor));
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

    @Override
    public CompletionStage<Terminated> getWhenTerminated() {
        return whenTerminated;
    }

    private void finishTermination(Terminated terminated, Throwable failure) {
        try {
            scheduler.close();
        } catch (RuntimeException error) {
            log.error(error, "Error during actor system termination");
        }
        if (failure == null) whenTerminated.complete(terminated);
        else whenTerminated.completeExceptionally(failure);
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
    public void close() {
        terminate();
        try {
            whenTerminated.get(settings.config().getDuration(ActorPath.Scheme + ".coordinated-shutdown.close-actor-system-timeout")
                    .toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while closing ActorSystem", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("ActorSystem did not terminate", error);
        }
    }
}
