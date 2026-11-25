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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.ActorKilledException;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.InvalidMessageException;
import io.github.espen7.tapio.actor.MinimalActorRef;
import io.github.espen7.tapio.actor.Status;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 代表一次 {@link Patterns#ask} 请求的临时回复引用。
 *
 * <p>runtime 创建此引用，并在收到首个回复或达到超时时停止它。存活期间首次调用
 * {@link #path()} 时，Provider 才会分配并注册路径。停止时会通知 DeathWatch
 * 观察者并注销已注册的路径。
 */
public final class PromiseActorRef extends MinimalActorRef {
    private static final Object Registering = new Object();
    private static final Object Stopped = new Object();
    private record StoppedWithPath(ActorPath path) {}
    private static final ActorKilledException ActorStopResult = new ActorKilledException("Stopped");

    private static final VarHandle stateHandle;
    private static final VarHandle watchedByHandle;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            stateHandle = lookup.findVarHandle(PromiseActorRef.class, "state", Object.class);
            watchedByHandle = lookup.findVarHandle(PromiseActorRef.class, "watchedBy", Set.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private final ActorRefProvider provider;
    private final CompletableFuture<Object> result;
    private final String messageClassName;
    private final String refPathPrefix;

    /* null 表示仍存活且尚未创建路径。其他状态与 Pekko 的状态机一致。 */
    private volatile Object state;
    /* null 表示引用已停止，后续 Watch 直接收到终止通知。 */
    private volatile Set<ActorRef> watchedBy = Set.of();

    private PromiseActorRef(ActorRefProvider provider, CompletableFuture<Object> result,
                            String messageClassName, String refPathPrefix) {
        this.provider = provider;
        this.result = result;
        this.messageClassName = messageClassName;
        this.refPathPrefix = refPathPrefix;
    }

    static PromiseActorRef apply(ActorRefProvider provider, Duration timeout, ActorRef targetName,
                                 String messageClassName, String refPathPrefix, ActorRef sender) {
        if (refPathPrefix.indexOf('/') >= 0) {
            throw new IllegalArgumentException("refPathPrefix must not contain slash, was: " + refPathPrefix);
        }
        CompletableFuture<Object> result = new CompletableFuture<>();
        PromiseActorRef ref = new PromiseActorRef(provider, result, messageClassName, refPathPrefix);
        Cancellable task = provider.scheduler().scheduleOnce(timeout, () -> {
            String wasSentBy = sender == noSender ? "" : " was sent by [" + sender + "]";
            boolean timedOut = result.completeExceptionally(new AskTimeoutException(
                    "Ask timed out on [" + targetName + "] after [" + timeout.toMillis() + " ms]. "
                            + "Message of type [" + ref.messageClassName + "]" + wasSentBy + "."
                            + " A typical reason for `AskTimeoutException` is that the recipient actor didn't send a reply."));
            if (timedOut) ref.onTimeout(timeout);
        });
        result.whenComplete((value, failure) -> {
            try {
                ref.stop();
            } finally {
                task.cancel();
            }
        });
        return ref;
    }

    CompletionStage<Object> ask(ActorRef actorRef, Object message) {
        actorRef.tell(message, this);
        return result.minimalCompletionStage();
    }

    @Override
    public ActorRefProvider provider() {
        return provider;
    }

    /**
     * 返回此临时引用在 Provider 中的父级。
     *
     * @return 所属 Provider 的临时 Actor 容器
     */
    public InternalActorRef getParent() {
        return provider.tempContainer();
    }

    /**
     * 返回此引用的稳定路径。若首次读取时引用已经停止，只分配路径。
     *
     * @return 此引用的临时 Actor 路径
     */
    @Override
    public ActorPath path() {
        for (;;) {
            Object current = state;
            if (current == null) {
                if (stateHandle.compareAndSet(this, null, Registering)) {
                    ActorPath path = null;
                    try {
                        path = provider.tempPath(refPathPrefix);
                        provider.registerTempActor(this, path);
                        return path;
                    } finally {
                        state = path;
                    }
                }
            } else if (current instanceof ActorPath path) {
                return path;
            } else if (current instanceof StoppedWithPath stopped) {
                return stopped.path();
            } else if (current == Stopped) {
                stateHandle.compareAndSet(this, Stopped, new StoppedWithPath(provider.tempPath(refPathPrefix)));
            } else if (current == Registering) {
                Thread.onSpinWait();
            } else {
                throw new IllegalStateException("Unexpected state: " + current);
            }
        }
    }

    /**
     * 尝试用回复完成 ask。迟到或重复的回复交给 dead letters。
     *
     * @param message 回复消息。{@link Status.Success} 提供成功值，{@link Status.Failure} 提供失败原因
     * @param sender 回复消息的发送者，可以为 {@link ActorRef#noSender}
     * @throws InvalidMessageException 临时引用仍存活且消息为 {@code null}
     */
    @Override
    public void tell(Object message, ActorRef sender) {
        Object current = state;
        if (current == Stopped || current instanceof StoppedWithPath) {
            provider.deadLetters().tell(new DeadLetter(message, sender == noSender ? provider.deadLetters() : sender, this));
            onComplete(message, true);
        } else {
            if (message == null) throw new InvalidMessageException("Message is null");
            boolean completed = switch (message) {
                case Status.Success success -> result.complete(success.status());
                case Status.Failure failure -> result.completeExceptionally(failure.cause());
                default -> result.complete(message);
            };
            if (!completed) provider.deadLetters().tell(message);
            onComplete(message, !completed);
        }
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        switch (message) {
            case SystemMessage.Terminate ignored -> stop();
            case SystemMessage.DeathWatchNotification notification ->
                    tell(new Terminated(notification.actor, notification.existenceConfirmed, notification.addressTerminated));
            case SystemMessage.Watch watch -> {
                if (watch.watchee.equals(this) && !watch.watcher.equals(this)) {
                    if (!addWatcher(watch.watcher)) {
                        ((InternalActorRef) watch.watcher).sendSystemMessage(
                                new SystemMessage.DeathWatchNotification(this, true, false));
                    }
                } else {
                    System.err.println("BUG: illegal Watch(" + watch.watchee + "," + watch.watcher + ") for " + this);
                }
            }
            case SystemMessage.Unwatch unwatch -> {
                if (unwatch.watchee.equals(this) && !unwatch.watcher.equals(this)) remWatcher(unwatch.watcher);
                else System.err.println("BUG: illegal Unwatch(" + unwatch.watchee + "," + unwatch.watcher + ") for " + this);
            }
            default -> {
            }
        }
    }

    private boolean addWatcher(ActorRef watcher) {
        for (;;) {
            Set<ActorRef> current = watchedBy;
            if (current == null) return false;
            Set<ActorRef> updated = new HashSet<>(current);
            updated.add(watcher);
            if (watchedByHandle.compareAndSet(this, current, Set.copyOf(updated))) return true;
        }
    }

    private void remWatcher(ActorRef watcher) {
        for (;;) {
            Set<ActorRef> current = watchedBy;
            if (current == null) return;
            Set<ActorRef> updated = new HashSet<>(current);
            updated.remove(watcher);
            if (watchedByHandle.compareAndSet(this, current, Set.copyOf(updated))) return;
        }
    }

    private Set<ActorRef> clearWatchers() {
        for (;;) {
            Set<ActorRef> current = watchedBy;
            if (current == null) return Set.of();
            if (watchedByHandle.compareAndSet(this, current, null)) return current;
        }
    }

    /**
     * 返回此临时引用是否已进入停止状态。
     *
     * @return 引用已进入停止状态时为 {@code true}
     */
    @Override
    public boolean isTerminated() {
        Object current = state;
        return current == Stopped || current instanceof StoppedWithPath;
    }

    /**
     * 停止此引用，使尚未完成的 ask 失败，通知观察者并注销已注册的路径。
     *
     * <p>可重复调用。只有首次进入停止状态的调用执行清理。
     */
    @Override
    public void stop() {
        for (;;) {
            Object current = state;
            if (current == null) {
                if (stateHandle.compareAndSet(this, null, Stopped)) {
                    ensureCompleted();
                    return;
                }
            } else if (current instanceof ActorPath path) {
                if (stateHandle.compareAndSet(this, path, new StoppedWithPath(path))) {
                    try {
                        ensureCompleted();
                    } finally {
                        provider.unregisterTempActor(path);
                    }
                    return;
                }
            } else if (current == Stopped || current instanceof StoppedWithPath) {
                return;
            } else if (current == Registering) {
                Thread.onSpinWait();
            } else {
                throw new IllegalStateException("Unexpected state: " + current);
            }
        }
    }

    private void ensureCompleted() {
        result.completeExceptionally(ActorStopResult);
        for (ActorRef watcher : clearWatchers()) {
            ((InternalActorRef) watcher).sendSystemMessage(new SystemMessage.DeathWatchNotification(this, true, false));
        }
    }

    void onComplete(Object message, boolean alreadyCompleted) {
    }

    void onTimeout(Duration timeout) {
    }
}
