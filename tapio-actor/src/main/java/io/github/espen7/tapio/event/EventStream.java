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
package io.github.espen7.tapio.event;

import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.actor.Actor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.InvalidActorNameException;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.UnhandledMessage;
import io.github.espen7.tapio.pattern.Patterns;
import io.github.espen7.tapio.util.NonFatal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ActorSystem 的事件发布、订阅和默认 logger 生命周期入口。
 *
 * <p>订阅者是 ActorRef，channel 是事件 class。订阅父类型时也会收到其子类型事件，
 * 发布通过 ActorRef.tell 投递，不会在发布线程中执行 subscriber 的业务逻辑。</p>
 */
public final class EventStream {
    private final Map<Class<?>, Set<ActorRef>> subscriptions = new HashMap<>();
    private final ReentrantLock guard = new ReentrantLock();
    private final LogExt logExt = new LogExt();
    private List<ActorRef> loggers = List.of();
    private volatile LogEvent.Level logLevel = LogEvent.Level.OFF;
    private boolean defaultLoggersStopped;
    private volatile Map<Class<?>, Set<ActorRef>> cache = Map.of();

    /** 创建尚未安装默认 logger 的 EventStream。 */
    public EventStream() {
    }

    /**
     * 创建 EventStream。
     *
     * @param logLevel 低于此级别的日志事件不会发送给默认 logger
     */
    public EventStream(LogEvent.Level logLevel) {
        this.logLevel = logLevel;
    }

    /**
     * 订阅指定 class 及其子类型的事件。
     *
     * @param subscriber 订阅者 ActorRef
     * @param channel 事件 channel class
     * @return 新增订阅时返回 {@code true}
     */
    public boolean subscribe(ActorRef subscriber, Class<?> channel) {
        if (subscriber == null) throw new IllegalArgumentException("subscriber is null");
        if (channel == null) throw new IllegalArgumentException("channel is null");
        synchronized (subscriptions) {
            boolean added = subscriptions.computeIfAbsent(channel, ignored -> new HashSet<>()).add(subscriber);
            if (added) updateCache();
            return added;
        }
    }

    /**
     * 取消指定 channel 的订阅。
     *
     * @param subscriber 订阅者 ActorRef
     * @param channel 事件 channel class
     * @return 存在并移除订阅时返回 {@code true}
     */
    public boolean unsubscribe(ActorRef subscriber, Class<?> channel) {
        if (subscriber == null) throw new IllegalArgumentException("subscriber is null");
        if (channel == null) throw new IllegalArgumentException("channel is null");
        synchronized (subscriptions) {
            Set<ActorRef> channelSubscribers = subscriptions.get(channel);
            if (channelSubscribers == null) return false;
            boolean removed = channelSubscribers.remove(subscriber);
            if (channelSubscribers.isEmpty()) subscriptions.remove(channel);
            if (removed) updateCache();
            return removed;
        }
    }

    /**
     * 取消 subscriber 的全部订阅。
     *
     * @param subscriber 要取消订阅的 ActorRef
     */
    public void unsubscribe(ActorRef subscriber) {
        if (subscriber == null) throw new IllegalArgumentException("subscriber is null");
        synchronized (subscriptions) {
            boolean removed = false;
            for (Set<ActorRef> channelSubscribers : subscriptions.values()) {
                removed |= channelSubscribers.remove(subscriber);
            }
            subscriptions.values().removeIf(Set::isEmpty);
            if (removed) updateCache();
        }
    }

    /**
     * 向订阅了事件 class 或其父类型 channel 的 Actor 投递事件。
     *
     * @param event 要发布的事件
     */
    public void publish(Object event) {
        if (event == null) throw new IllegalArgumentException("event is null");
        Class<?> eventClass = event.getClass();
        Map<Class<?>, Set<ActorRef>> currentCache = cache;
        Set<ActorRef> recipients = currentCache.get(eventClass);
        if (recipients == null) {
            synchronized (subscriptions) {
                currentCache = cache;
                recipients = currentCache.get(eventClass);
                if (recipients == null) {
                    recipients = subscribersFor(eventClass);
                    Map<Class<?>, Set<ActorRef>> updated = new HashMap<>(currentCache);
                    updated.put(eventClass, recipients);
                    cache = Map.copyOf(updated);
                }
            }
        }
        for (ActorRef recipient : recipients) recipient.tell(event);
    }

    /**
     * 返回当前日志级别阈值。
     *
     * @return 日志级别阈值
     */
    public LogEvent.Level logLevel() {
        return logLevel;
    }

    /**
     * 调整默认 logger 的级别订阅。
     *
     * @param level 新的日志级别阈值
     */
    public void setLogLevel(LogEvent.Level level) {
        if (level == null) throw new IllegalArgumentException("level is null");
        guard.lock();
        try {
            for (LogEvent.Level channel : LogEvent.Level.values()) {
                if (channel == LogEvent.Level.OFF) continue;
                boolean enabled = level != LogEvent.Level.OFF && channel.ordinal() >= level.ordinal();
                boolean previouslyEnabled = logLevel != LogEvent.Level.OFF
                        && channel.ordinal() >= logLevel.ordinal();
                if (enabled && !previouslyEnabled) {
                    for (ActorRef logger : loggers) subscribe(logger, classFor(channel));
                } else if (!enabled && previouslyEnabled) {
                    for (ActorRef logger : loggers) unsubscribe(logger, classFor(channel));
                }
            }
            logLevel = level;
        } finally {
            guard.unlock();
        }
    }

    /**
     * 判断指定日志级别是否启用。
     *
     * @param level 待判断的日志级别
     * @return 启用时返回 {@code true}
     */
    public boolean isEnabled(LogEvent.Level level) {
        return level != LogEvent.Level.OFF && logLevel != LogEvent.Level.OFF
                && level.ordinal() >= logLevel.ordinal();
    }

    /**
     * INTERNAL API：安装 ActorSystem 启动阶段的标准输出 logger。
     *
     * @param settings ActorSystem 的有效设置
     */
    @InternalApi
    public void startStdoutLogger(ActorSystem.Settings settings) {
        setUpStdoutLogger(settings);
        publish(new Debug("EventStream", EventStream.class, "StandardOutLogger started"));
    }

    private void setUpStdoutLogger(ActorSystem.Settings settings) {
        LogEvent.Level level = levelFor(settings.StdoutLogLevel).orElseGet(() -> {
            StandardOutLogger.INSTANCE.print(new Error("EventStream", EventStream.class,
                    "unknown tapio.stdout-loglevel " + settings.StdoutLogLevel, new LoggerException()));
            return LogEvent.Level.ERROR;
        });
        for (LogEvent.Level channel : LogEvent.Level.values()) {
            if (level != LogEvent.Level.OFF && channel != LogEvent.Level.OFF && channel.ordinal() >= level.ordinal()) {
                subscribe(StandardOutLogger.INSTANCE, classFor(channel));
            }
        }
        guard.lock();
        try {
            List<ActorRef> updated = new ArrayList<>(loggers);
            updated.add(StandardOutLogger.INSTANCE);
            loggers = List.copyOf(updated);
            logLevel = level;
        } finally {
            guard.unlock();
        }
    }

    /**
     * INTERNAL API：创建、初始化并发布配置的默认 logger。
     *
     * @param system 所属 ActorSystem
     */
    @InternalApi
    public void startDefaultLoggers(ActorSystemImpl system) {
        String logName = "EventStream(" + ActorPath.Scheme + "://" + system.name() + ")";
        LogEvent.Level level = levelFor(system.settings().LogLevel).orElseGet(() -> {
            StandardOutLogger.INSTANCE.print(new Error(logName, EventStream.class,
                    "unknown tapio.loglevel " + system.settings().LogLevel, new LoggerException()));
            return LogEvent.Level.ERROR;
        });
        List<ActorRef> started = new ArrayList<>();
        List<ActorRef> previousLoggers = List.of();
        LogEvent.Level previousLogLevel = LogEvent.Level.ERROR;
        boolean published = false;
        try {
            List<String> configured = system.settings().Loggers;
            List<String> defaultLoggers = configured.isEmpty() ? List.of(DefaultLogger.class.getName()) : configured;
            List<String> loggerNames = defaultLoggers.stream()
                    .filter(name -> !name.equals(StandardOutLogger.class.getName())).toList();
            List<Class<? extends Actor>> loggerClasses = new ArrayList<>();
            for (String loggerName : loggerNames) {
                try {
                    loggerClasses.add(Class.forName(loggerName, false, system.settings().classLoader())
                            .asSubclass(Actor.class));
                } catch (ReflectiveOperationException | ClassCastException error) {
                    throw new ConfigurationException("Logger specified in config can't be loaded [" + loggerName
                            + "] due to [" + error + "]", error);
                }
            }
            for (Class<? extends Actor> loggerClass : loggerClasses) {
                started.add(addLogger(system, loggerClass, level, logName));
            }
            guard.lock();
            try {
                if (!defaultLoggersStopped) {
                    previousLoggers = loggers;
                    previousLogLevel = logLevel;
                    loggers = List.copyOf(started);
                    logLevel = level;
                    published = true;
                }
            } finally {
                guard.unlock();
            }
            if (!published) {
                started.forEach(this::stopLogger);
            } else {
                if (system.settings().DebugUnhandledMessage) {
                    try {
                        subscribe(system.systemActorOf(Props.create(UnhandledMessageForwarder.class),
                                "UnhandledMessageForwarder"), UnhandledMessage.class);
                    } catch (InvalidActorNameException ignored) {
                    }
                }
                publish(new Debug(logName, EventStream.class, "Default Loggers started"));
                if (!defaultLoggers.contains(StandardOutLogger.class.getName())) unsubscribe(StandardOutLogger.INSTANCE);
            }
        } catch (Exception error) {
            if (published) restorePreviousLoggers(started, previousLoggers, previousLogLevel);
            for (ActorRef logger : started) stopLoggerSafely(logger, error);
            System.err.println("error while starting up loggers");
            error.printStackTrace();
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new ConfigurationException("Could not start logger due to [" + error + "]", error);
        } catch (java.lang.Error error) {
            if (!NonFatal.apply(error)) throw error;
            if (published) restorePreviousLoggers(started, previousLoggers, previousLogLevel);
            for (ActorRef logger : started) stopLoggerSafely(logger, error);
            throw error;
        }
    }

    /**
     * INTERNAL API：恢复标准输出兜底，先退订再停止默认 logger。
     *
     * @param system 所属 ActorSystem
     */
    @InternalApi
    public void stopDefaultLoggers(ActorSystem system) {
        List<ActorRef> currentLoggers;
        guard.lock();
        try {
            defaultLoggersStopped = true;
            currentLoggers = loggers;
        } finally {
            guard.unlock();
        }
        if (!currentLoggers.contains(StandardOutLogger.INSTANCE)) {
            setUpStdoutLogger(system.settings());
            publish(new Debug("EventStream", EventStream.class, "shutting down: StandardOutLogger"));
        }
        for (ActorRef logger : currentLoggers) {
            if (logger != StandardOutLogger.INSTANCE) stopLogger(logger);
        }
        publish(new Debug("EventStream", EventStream.class, "all default loggers stopped"));
    }

    private void restorePreviousLoggers(List<ActorRef> started, List<ActorRef> previousLoggers,
                                        LogEvent.Level previousLogLevel) {
        guard.lock();
        try {
            List<ActorRef> restored = new ArrayList<>(previousLoggers);
            for (ActorRef logger : loggers) {
                if (!started.contains(logger) && !restored.contains(logger)) restored.add(logger);
            }
            loggers = List.copyOf(restored);
            logLevel = previousLogLevel;
        } finally {
            guard.unlock();
        }
    }

    private ActorRef addLogger(ActorSystemImpl system, Class<? extends Actor> clazz, LogEvent.Level level,
                               String logName) throws InterruptedException {
        String name = "log" + logExt.id() + "-" + Logging.simpleName(clazz);
        ActorRef actor = system.systemActorOf(Props.create(clazz).withDispatcher(system.settings().LoggersDispatcher), name);
        try {
            Object response;
            try {
                response = Patterns.ask(actor, new InitializeLogger(this), system.settings().LoggerStartTimeout)
                        .toCompletableFuture().get(system.settings().LoggerStartTimeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException timeout) {
                response = timeoutResponse(system, logName, name);
            } catch (ExecutionException error) {
                Throwable cause = error.getCause();
                if (cause instanceof TimeoutException) {
                    response = timeoutResponse(system, logName, name);
                } else if (cause instanceof RuntimeException runtimeError) {
                    throw runtimeError;
                } else if (cause instanceof java.lang.Error fatal) {
                    throw fatal;
                } else {
                    throw new IllegalStateException(cause);
                }
            }
            if (response != LoggerInitialized.getInstance()) {
                throw new LoggerInitializationException("Logger " + name
                        + " did not respond with LoggerInitialized, sent instead " + response);
            }
            for (LogEvent.Level channel : LogEvent.Level.values()) {
                if (level != LogEvent.Level.OFF && channel != LogEvent.Level.OFF && channel.ordinal() >= level.ordinal()) {
                    subscribe(actor, classFor(channel));
                }
            }
            publish(new Debug(logName, EventStream.class, "logger " + name + " started"));
            return actor;
        } catch (Exception error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            stopLoggerSafely(actor, error);
            throw error;
        } catch (java.lang.Error error) {
            if (!NonFatal.apply(error)) throw error;
            stopLoggerSafely(actor, error);
            throw error;
        }
    }

    private String timeoutResponse(ActorSystemImpl system, String logName, String name) {
        publish(new Warning(logName, EventStream.class, "Logger " + name + " did not respond within Timeout("
                + system.settings().LoggerStartTimeout.toMillis() + " milliseconds) to InitializeLogger(bus)"));
        return "[TIMEOUT]";
    }

    private void stopLogger(ActorRef logger) {
        unsubscribe(logger);
        if (logger instanceof InternalActorRef ref) ref.stop();
    }

    private void stopLoggerSafely(ActorRef logger, Throwable cause) {
        try {
            stopLogger(logger);
        } catch (RuntimeException | java.lang.Error cleanup) {
            if (NonFatal.apply(cleanup)) cause.addSuppressed(cleanup);
            else throw cleanup;
        }
    }

    private void updateCache() {
        Map<Class<?>, Set<ActorRef>> updated = new HashMap<>();
        for (Class<?> eventClass : cache.keySet()) {
            updated.put(eventClass, subscribersFor(eventClass));
        }
        cache = Map.copyOf(updated);
    }

    private Set<ActorRef> subscribersFor(Class<?> eventClass) {
        Set<ActorRef> result = new HashSet<>();
        subscriptions.forEach((channel, subscribers) -> {
            if (channel.isAssignableFrom(eventClass)) result.addAll(subscribers);
        });
        return Set.copyOf(result);
    }

    private static Class<?> classFor(LogEvent.Level level) {
        return switch (level) {
            case DEBUG -> Debug.class;
            case INFO -> Info.class;
            case WARNING -> Warning.class;
            case ERROR -> Error.class;
            case OFF -> throw new IllegalArgumentException("OFF is not an event channel");
        };
    }

    private static java.util.Optional<LogEvent.Level> levelFor(String value) {
        try {
            return java.util.Optional.of(LogEvent.Level.parse(value));
        } catch (IllegalArgumentException ignored) {
            return java.util.Optional.empty();
        }
    }
}
