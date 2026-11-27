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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.util.Subclassification;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ActorSystem 的事件发布、订阅和默认 logger 生命周期入口。
 *
 * <p>订阅者是 ActorRef，channel 是事件 class。订阅父类型时也会收到其子类型事件，
 * 发布通过 ActorRef.tell 投递，不会在发布线程中执行 subscriber 的业务逻辑。</p>
 */
public class EventStream extends SubchannelClassification {
    private final ActorSystem system;
    private final boolean debug;
    private final AtomicReference<UnsubscriberState> initiallySubscribedOrUnsubscriber =
            new AtomicReference<>(new InitialSubscriptions(Set.of()));
    @Override
    protected final Subclassification<Class<?>> subclassification() {
        return new Subclassification<>() {
            @Override
            public boolean isEqual(Class<?> left, Class<?> right) {
                return left == right;
            }

            @Override
            public boolean isSubclass(Class<?> left, Class<?> right) {
                return right.isAssignableFrom(left);
            }
        };
    }

    /** 创建关闭订阅调试日志的 EventStream。 */
    public EventStream(ActorSystem system) {
        this(system, false);
    }

    /**
     * 创建绑定 ActorSystem 的 EventStream。
     *
     * @param system 所属 ActorSystem；为 {@code null} 时不启用自动退订
     * @param debug 是否发布订阅和退订操作的 DEBUG 事件
     */
    public EventStream(ActorSystem system, boolean debug) {
        this.system = system;
        this.debug = debug;
    }

    private sealed interface UnsubscriberState permits InitialSubscriptions, ActiveUnsubscriber {
    }

    private record InitialSubscriptions(Set<ActorRef> subscribers) implements UnsubscriberState {
    }

    private record ActiveUnsubscriber(ActorRef unsubscriber) implements UnsubscriberState {
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
        if (debug) {
            publish(new Debug(Logging.simpleName(this), getClass(),
                    "subscribing " + subscriber + " to channel " + channel));
        }
        registerWithUnsubscriber(subscriber);
        return super.subscribe(subscriber, channel);
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
        boolean removed = super.unsubscribe(subscriber, channel);
        unregisterIfNoMoreSubscribedChannels(subscriber);
        if (debug) {
            publish(new Debug(Logging.simpleName(this), getClass(),
                    "unsubscribing " + subscriber + " from channel " + channel));
        }
        return removed;
    }

    /**
     * 取消 subscriber 的全部订阅。
     *
     * @param subscriber 要取消订阅的 ActorRef
     */
    public void unsubscribe(ActorRef subscriber) {
        if (subscriber == null) throw new IllegalArgumentException("subscriber is null");
        super.unsubscribe(subscriber);
        unregisterIfNoMoreSubscribedChannels(subscriber);
        if (debug) {
            publish(new Debug(Logging.simpleName(this), getClass(),
                    "unsubscribing " + subscriber + " from all channels"));
        }
    }

    /**
     * INTERNAL API：ActorSystem 就绪后启动自动退订订阅者的系统 Actor。
     */
    @InternalApi
    public void startUnsubscriber() {
        if (system != null) EventStreamUnsubscriber.start(system, this);
    }

    boolean initUnsubscriber(ActorRef unsubscriber) {
        if (system == null) return false;
        for (; ; ) {
            UnsubscriberState current = initiallySubscribedOrUnsubscriber.get();
            if (current instanceof ActiveUnsubscriber present) {
                if (debug) {
                    publish(new Debug(Logging.simpleName(this), getClass(),
                            "not using unsubscriber " + unsubscriber + ", because already initialized with "
                                    + present.unsubscriber()));
                }
                return false;
            }
            InitialSubscriptions initial = (InitialSubscriptions) current;
            if (initiallySubscribedOrUnsubscriber.compareAndSet(current, new ActiveUnsubscriber(unsubscriber))) {
                if (debug) {
                    publish(new Debug(Logging.simpleName(this), getClass(),
                            "initialized unsubscriber to: " + unsubscriber + ", registering "
                                    + initial.subscribers().size() + " initial subscribers with it"));
                }
                initial.subscribers().forEach(this::registerWithUnsubscriber);
                return true;
            }
        }
    }

    private void registerWithUnsubscriber(ActorRef subscriber) {
        if (system == null) return;
        for (; ; ) {
            UnsubscriberState current = initiallySubscribedOrUnsubscriber.get();
            if (current instanceof InitialSubscriptions initial) {
                Set<ActorRef> updated = new HashSet<>(initial.subscribers());
                updated.add(subscriber);
                if (!initiallySubscribedOrUnsubscriber.compareAndSet(current,
                        new InitialSubscriptions(Set.copyOf(updated)))) {
                    continue;
                }
                return;
            }
            ((ActiveUnsubscriber) current).unsubscriber()
                    .tell(new EventStreamUnsubscriber.Register(subscriber), ActorRef.noSender);
            return;
        }
    }

    private void unregisterIfNoMoreSubscribedChannels(ActorRef subscriber) {
        if (system == null) return;
        for (; ; ) {
            UnsubscriberState current = initiallySubscribedOrUnsubscriber.get();
            if (current instanceof InitialSubscriptions initial) {
                Set<ActorRef> updated = new HashSet<>(initial.subscribers());
                updated.remove(subscriber);
                if (!initiallySubscribedOrUnsubscriber.compareAndSet(current,
                        new InitialSubscriptions(Set.copyOf(updated)))) {
                    continue;
                }
                return;
            }
            ((ActiveUnsubscriber) current).unsubscriber()
                    .tell(new EventStreamUnsubscriber.UnregisterIfNoMoreSubscribedChannels(subscriber), ActorRef.noSender);
            return;
        }
    }

    @Override
    protected Class<?> classify(Object event) {
        return event.getClass();
    }

    @Override
    protected void publish(Object event, ActorRef subscriber) {
        if (system == null && subscriber instanceof InternalActorRef internal && internal.isTerminated()) unsubscribe(subscriber);
        else subscriber.tell(event, ActorRef.noSender);
    }
}
