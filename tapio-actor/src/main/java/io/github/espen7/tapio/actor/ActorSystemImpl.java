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
import io.github.espen7.tapio.dispatch.Dispatcher;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.event.DeadLetterListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ActorSystem 的本地实现。
 *
 * <p>初始化顺序先准备 Dispatcher、EventStream 和 Scheduler，再由 Provider 创建 user
 * guardian。所有顶层 Actor 都挂在 user guardian 下面，关闭时从 guardian 开始停止整棵
 * Actor tree。</p>
 */
public class ActorSystemImpl extends ActorSystem {

    private static final String ACTOR_NAME_PATTERN = "^[a-zA-Z0-9][a-zA-Z0-9-_]*$";

    /** ActorSystem 的逻辑名称，同时用于生成 ActorPath 和线程名称。 */
    private final String name;
    /** 保存合并后的系统配置和值已解析的 runtime 设置。 */
    private final Settings settings;
    /** 根据配置创建并管理此系统的 Dispatcher。 */
    private final Dispatchers dispatchers;
    /** 负责提交 Actor mailbox 任务的共享 Dispatcher。 */
    private final Dispatcher dispatcher;
    /** 负责一次性调度和关闭阶段定时任务的共享 Scheduler。 */
    private final Scheduler scheduler;
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
        this.eventStream = new EventStream(LogEvent.Level.parse(settings.LogLevel));
        this.log = Logging.getLogger(eventStream, "pekko://" + name, getClass());
        this.dispatchers = new Dispatchers(settings, this);
        Dispatcher startedDispatcher = null;
        Scheduler startedScheduler = null;
        CoordinatedShutdown startedShutdown = null;
        try {
            this.dispatcher = startedDispatcher = dispatchers.defaultGlobalDispatcher();
            this.scheduler = startedScheduler = createScheduler();
            this.coordinatedShutdown = startedShutdown = new CoordinatedShutdown(this);
            this.provider = new LocalActorRefProvider(this);
            Logging.installSlf4jLogger(this, debugUnhandled());
            if (settings.LogDeadLetters > 0) {
                this.logDeadLetterListener = systemActorOf(Props.create(DeadLetterListener.class,
                        settings.LogDeadLetters, settings.LogDeadLettersSuspendDuration,
                        settings.logDeadLettersSuspendDurationDescription), "deadLetterListener");
            } else {
                this.logDeadLetterListener = null;
            }
            if (settings.CoordinatedShutdownTerminateActorSystem) {
                coordinatedShutdown.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "terminate-system", () -> {
                    finalTerminate();
                    return whenTerminated.thenApply(ignored -> Done.done());
                });
            }
        } catch (RuntimeException error) {
            if (startedShutdown != null) startedShutdown.cancelJvmShutdownHook();
            try {
                if (startedScheduler != null) startedScheduler.close();
            } catch (RuntimeException closeError) {
                error.addSuppressed(closeError);
            } finally {
                if (startedDispatcher != null) startedDispatcher.close();
            }
            throw error;
        }
    }

    /**
     * 按 {@code pekko.scheduler.implementation} 创建并启动系统调度器。
     *
     * @return 为当前系统创建的调度器
     * @throws IllegalArgumentException 无法加载或构造所配置的调度器
     */
    protected Scheduler createScheduler() {
        String className = settings.SchedulerClass;
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        boolean daemonic = settings.Daemonicity;
        AtomicLong threadNumber = new AtomicLong();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = defaultFactory.newThread(runnable);
            thread.setName(name + "-scheduler-" + threadNumber.incrementAndGet());
            thread.setDaemon(daemonic);
            return thread;
        };
        try {
            return Class.forName(className).asSubclass(Scheduler.class)
                    .getConstructor(Config.class, LoggingAdapter.class, ThreadFactory.class)
                    .newInstance(settings.config(), log, threadFactory);
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

    @Override
    public ActorRef actorOf(Props props, String name) {
        return ((LocalActorRef) provider.rootGuardian()).underlying().attachChild(props, name, false);
    }

    @Override
    public ActorRef systemActorOf(Props props, String name) {
        return provider.systemActorOf(props, name);
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
    public Dispatcher dispatcher() {
        return dispatcher;
    }

    @Override
    public Dispatchers dispatchers() {
        return dispatchers;
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
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

    void actorTerminated(ActorRef actor) {
        if (!actor.equals(provider.rootGuardian())) return;
        provider.systemGuardian().stop();
    }

    void systemGuardianTerminated(ActorRef actor) {
        if (!actor.equals(provider.systemGuardian())) return;
        finishTermination(provider.rootGuardian());
    }

    private void finishTermination(ActorRef actor) {
        try {
            scheduler.close();
        } catch (RuntimeException error) {
            log.error(error, "Error during actor system termination");
        }
        whenTerminated.complete(new Terminated(actor));
    }

    void finalTerminate() {
        if (!settings.LogDeadLettersDuringShutdown && logDeadLetterListener != null) {
            logDeadLetterListener.stop();
        }
        provider.rootGuardian().stop();
        coordinatedShutdown.cancelJvmShutdownHook();
    }

    /** 按 Pekko close 超时等待 ActorSystem 完成终止。 */
    @Override
    public void close() {
        terminate();
        try {
            whenTerminated.get(settings.config().getDuration("pekko.coordinated-shutdown.close-actor-system-timeout")
                    .toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while closing ActorSystem", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("ActorSystem did not terminate", error);
        }
    }
}
