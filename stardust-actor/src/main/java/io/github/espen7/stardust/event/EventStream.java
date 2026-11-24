package io.github.espen7.stardust.event;

import io.github.espen7.stardust.actor.ActorRef;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ActorSystem 的事件发布和订阅入口。
 *
 * <p>订阅者是 ActorRef，channel 是事件 class。订阅父类型时也会收到其子类型事件，
 * 发布通过 ActorRef.tell 投递，不会在发布线程中执行 subscriber 的业务逻辑。</p>
 */
public final class EventStream {
    private final Map<Class<?>, Set<ActorRef>> subscriptions = new HashMap<>();
    private final LogEvent.Level logLevel;
    private volatile Map<Class<?>, Set<ActorRef>> cache = Map.of();

    public EventStream(LogEvent.Level logLevel) {
        this.logLevel = logLevel;
    }

    /**
     * 订阅指定 class 及其子类型的事件。
     *
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

    public LogEvent.Level logLevel() {
        return logLevel;
    }

    public boolean isEnabled(LogEvent.Level level) {
        return level.ordinal() >= logLevel.ordinal();
    }
}
