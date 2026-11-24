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
package io.github.espen7.stardust.actor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.stardust.Done;
import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.event.EventStream;
import io.github.espen7.stardust.event.LogEvent;
import io.github.espen7.stardust.event.Logging;
import io.github.espen7.stardust.event.LoggingAdapter;
import io.github.espen7.stardust.event.DeadLetterListener;

import java.time.Duration;
import java.util.Locale;
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

    private final String name;
    private final Config config;
    private final Dispatcher dispatcher;
    private final Scheduler scheduler;
    private final EventStream eventStream;
    private final LoggingAdapter log;
    private final LocalActorRefProvider provider;
    private final ActorRef logDeadLetterListener;
    private final CoordinatedShutdown coordinatedShutdown;
    private final CompletableFuture<Terminated> whenTerminated = new CompletableFuture<>();

    /**
     * 创建本地 ActorSystem。
     *
     * @param name system name
     * @throws IllegalArgumentException 当名称不符合 Pekko 命名规则时
     */
    public ActorSystemImpl(String name) {
        if (name == null || !name.matches("^[a-zA-Z0-9][a-zA-Z0-9-_]*$")) {
            throw new IllegalArgumentException(
                    "invalid ActorSystem name [" + name
                            + "], must contain only word characters (i.e. [a-zA-Z0-9] plus non-leading '-' or '_')");
        }
        this.name = name;
        this.config = ConfigFactory.load();
        this.dispatcher = new Dispatcher(config.getInt("pekko.actor.default-dispatcher.throughput"), this);
        this.eventStream = new EventStream(LogEvent.Level.parse(config.getString("pekko.actor.loglevel")));
        this.log = Logging.getLogger(eventStream, "pekko://" + name, getClass());
        this.scheduler = createScheduler();
        this.coordinatedShutdown = new CoordinatedShutdown(this);
        try {
            this.provider = new LocalActorRefProvider(this);
            Logging.installSlf4jLogger(this, debugUnhandled());
            String configured = config.getString("pekko.log-dead-letters").toLowerCase(Locale.ROOT);
            int maxCount = switch (configured) {
                case "on", "true" -> Integer.MAX_VALUE;
                case "off", "false" -> 0;
                default -> Integer.parseInt(configured);
            };
            if (maxCount > 0) {
                String suspend = config.getString("pekko.log-dead-letters-suspend-duration");
                Duration duration = suspend.equalsIgnoreCase("infinite") ? null
                        : config.getDuration("pekko.log-dead-letters-suspend-duration");
                this.logDeadLetterListener = systemActorOf(Props.create(DeadLetterListener.class, maxCount, duration, suspend),
                        "deadLetterListener");
            } else {
                this.logDeadLetterListener = null;
            }
            if (config.getBoolean("pekko.coordinated-shutdown.terminate-actor-system")) {
                coordinatedShutdown.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "terminate-system", () -> {
                    finalTerminate();
                    return whenTerminated.thenApply(ignored -> Done.done());
                });
            }
        } catch (RuntimeException error) {
            coordinatedShutdown.cancelJvmShutdownHook();
            try {
                scheduler.close();
            } catch (RuntimeException closeError) {
                error.addSuppressed(closeError);
            } finally {
                dispatcher.close();
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
        String className = config.getString("pekko.scheduler.implementation");
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        boolean daemonic = config.getBoolean("pekko.daemonic");
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
                    .newInstance(config, log, threadFactory);
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

    Config config() {
        return config;
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
        return config.getBoolean("pekko.actor.debug.lifecycle");
    }

    boolean debugUnhandled() {
        return config.getBoolean("pekko.actor.debug.unhandled");
    }

    /**
     * 发起系统关闭。
     *
     * <p>Terminate 是幂等的。Actor tree 先结束，Scheduler 和 Dispatcher 随后
     * 释放，最后完成 whenTerminated。</p>
     */
    @Override
    public CompletionStage<Terminated> terminate() {
        if (config.getBoolean("pekko.coordinated-shutdown.run-by-actor-system-terminate")) {
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
        dispatcher.close();
        whenTerminated.complete(new Terminated(actor));
    }

    void finalTerminate() {
        if (!config.getBoolean("pekko.log-dead-letters-during-shutdown") && logDeadLetterListener != null) {
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
            whenTerminated.get(config.getDuration("pekko.coordinated-shutdown.close-actor-system-timeout")
                    .toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while closing ActorSystem", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("ActorSystem did not terminate", error);
        }
    }
}
