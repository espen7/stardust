package io.github.espen7.stardust.event;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * ActorSystem 的事件发布和订阅入口。
 *
 * <p>日志 subscriber 收到达到系统 log level 的 {@link LogEvent}；其他事件仍可由应用订阅。
 * 事件按发布顺序在独立线程中分发，subscriber 不会占用 Actor 的 mailbox 线程。</p>
 */
public final class EventStream implements AutoCloseable {
    private final CopyOnWriteArrayList<Consumer<Object>> subscribers = new CopyOnWriteArrayList<>();
    private final LogEvent.Level logLevel;
    private final ExecutorService executor;

    public EventStream(LogEvent.Level logLevel) {
        this.logLevel = Objects.requireNonNull(logLevel, "logLevel");
        this.executor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "stardust-eventstream");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * 订阅 EventStream 中发布的事件。
     *
     * @param subscriber 接收事件的 callback
     * @return 本次调用新增订阅时返回 {@code true}
     */
    public boolean subscribe(Consumer<Object> subscriber) {
        return subscribers.addIfAbsent(Objects.requireNonNull(subscriber, "subscriber"));
    }

    /**
     * 移除指定订阅。
     */
    public boolean unsubscribe(Consumer<Object> subscriber) {
        return subscribers.remove(subscriber);
    }

    /**
     * 发布一个事件。
     *
     * <p>低于系统 log level 的 LogEvent 会被过滤；其他事件不经过日志级别过滤。</p>
     */
    public void publish(Object event) {
        Objects.requireNonNull(event, "event");
        if (event instanceof LogEvent logEvent && !isEnabled(logEvent.level())) return;
        List<Consumer<Object>> currentSubscribers = List.copyOf(subscribers);
        try {
            executor.execute(() -> dispatch(event, currentSubscribers));
        } catch (RejectedExecutionException ignored) {
            // ActorSystem shutdown has stopped accepting events.
        }
    }

    private static void dispatch(Object event, List<Consumer<Object>> currentSubscribers) {
        for (Consumer<Object> subscriber : currentSubscribers) {
            try {
                subscriber.accept(event);
            } catch (RuntimeException ignored) {
                // 日志或事件订阅者不能让 Actor 的消息处理失败。
            }
        }
    }

    /**
     * 返回当前系统的最低发布级别。
     */
    public LogEvent.Level logLevel() {
        return logLevel;
    }

    /**
     * 判断指定日志级别是否会通过系统级别过滤。
     */
    public boolean isEnabled(LogEvent.Level level) {
        return level.ordinal() >= logLevel.ordinal();
    }

    /**
     * 停止接收事件并等待已发布事件完成分发。
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) executor.shutdownNow();
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
