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

import io.github.espen7.tapio.actor.dungeon.Children;
import io.github.espen7.tapio.actor.dungeon.ChildrenContainer;
import io.github.espen7.tapio.actor.dungeon.DeathWatch;
import io.github.espen7.tapio.actor.dungeon.Dispatch;
import io.github.espen7.tapio.actor.dungeon.FaultHandling;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.annotation.InternalStableApi;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.sysmsg.Create;
import io.github.espen7.tapio.dispatch.sysmsg.DeathWatchNotification;
import io.github.espen7.tapio.dispatch.sysmsg.Failed;
import io.github.espen7.tapio.dispatch.sysmsg.NoMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Recreate;
import io.github.espen7.tapio.dispatch.sysmsg.Resume;
import io.github.espen7.tapio.dispatch.sysmsg.StashWhenFailed;
import io.github.espen7.tapio.dispatch.sysmsg.StashWhenWaitingForChildren;
import io.github.espen7.tapio.dispatch.sysmsg.Supervise;
import io.github.espen7.tapio.dispatch.sysmsg.Suspend;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Terminate;
import io.github.espen7.tapio.dispatch.sysmsg.Unwatch;
import io.github.espen7.tapio.dispatch.sysmsg.Watch;
import io.github.espen7.tapio.event.Debug;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.Warning;
import io.github.espen7.tapio.japi.pf.PartialFunction;
import io.github.espen7.tapio.util.Helpers;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 保存 Actor 实例及其运行时状态。
 *
 * <p>ActorRef 是公开的地址，ActorCell 才拥有 Actor 实例、Mailbox、children、
 * watcher 和当前消息。所有用户消息都在 Dispatcher 调度的 Mailbox 线程中进入
 * {@link #invoke(Envelope)}，因此 Actor 的 receive 回调在同一时间只会有一个执行者。</p>
 */
@InternalApi
public class ActorCell extends Dispatch implements AbstractActor.ActorContext,
        io.github.espen7.tapio.actor.dungeon.ReceiveTimeout, Children, DeathWatch, FaultHandling {
    private static final Props terminatedProps = Props.create(Actor.class, () -> {
        throw new IllegalActorStateException("This Actor has been terminated");
    });
    private static final int DefaultState = 0;
    private static final int SuspendedState = 1;
    private static final int SuspendedWaitForChildrenState = 2;
    private static final Cancellable emptyCancellable = new Cancellable() {
        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };
    /** 区分默认 Terminated 与 watchWith 的自定义 null 消息。 */
    private static final Object NullCustomWatchMessage = new Object();

    private static final VarHandle childrenRefsHandle;
    private static final VarHandle nextNameHandle;

    static {
        try {
            childrenRefsHandle = MethodHandles.lookup().findVarHandle(
                    ActorCell.class, "childrenRefs", ChildrenContainer.class);
            nextNameHandle = MethodHandles.lookup().findVarHandle(ActorCell.class, "nextName", long.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    /** 所属 ActorSystem。 */
    private final ActorSystemImpl system;
    /** 此 cell 的 ActorRef。 */
    private final InternalActorRef self;
    /** 创建或重建 Actor 实例时使用的 Props。 */
    private Props _props;
    /** 调度此 cell mailbox 的 Dispatcher。 */
    private final MessageDispatcher dispatcher;
    /** 此 Actor 的 parent 和 supervisor。 */
    private final InternalActorRef parent;
    @SuppressWarnings("unused")
    private volatile long nextName;
    /** children 状态快照通过 CAS 更新，供运行时并发读取。 */
    @SuppressWarnings("FieldMayBeFinal")
    private volatile ChildrenContainer childrenRefs = ChildrenContainer.empty();
    /** 当前 watch 本 Actor 的引用快照，由系统消息处理路径更新。 */
    private Set<ActorRef> watchedBy = Set.of();
    private final Map<ActorRef, Optional<Object>> watching = new HashMap<>();
    private final Map<ActorRef, Optional<Object>> terminatedQueued = new HashMap<>();
    private SystemMessage sysmsgStash;
    /** 保存当前 behavior 及其被压入的旧 behavior。 */
    private final ArrayDeque<PartialFunction<Object, Void>> behaviorStack = new ArrayDeque<>();
    private ReceiveTimeoutState receiveTimeoutData;
    /** Actor 构造期间暂存 ActorContext。 */
    static final ThreadLocal<LinkedList<ActorContext>> contextStack = ThreadLocal.withInitial(LinkedList::new);
    private Actor _actor;
    /** 当前处理的 Envelope，供 ActorContext.sender() 读取。 */
    private volatile Envelope currentMessage;
    private FailedInfo _failed = NoFailedInfo.INSTANCE;

    /**
     * 创建 ActorCell 并绑定所属 runtime 组件。
     *
     * <p>Mailbox 由 {@link #init(boolean, io.github.espen7.tapio.dispatch.MailboxType)} 创建，
     * 因此构造函数不建立消息队列。</p>
     *
     * @param system     所属 ActorSystem
     * @param self       此 cell 对应的本地 ActorRef
     * @param props      创建或重新创建 Actor 实例所用的 Props
     * @param dispatcher 调度此 cell mailbox 的 Dispatcher
     * @param parent     负责监督该 cell 的父级引用
     */
    public ActorCell(ActorSystemImpl system, InternalActorRef self, Props props, MessageDispatcher dispatcher, InternalActorRef parent) {
        this.system = system;
        this.self = self;
        this._props = props;
        this.dispatcher = dispatcher;
        this.parent = parent;
    }

    /**
     * 返回此 ActorCell 对应的 ActorRef。
     *
     * @return 此 ActorCell 的 ActorRef
     */
    public InternalActorRef self() {
        return self;
    }

    /**
     * 返回当前 Actor 实例。
     *
     * @return 当前 Actor 实例；Create 完成前可能为 {@code null}
     */
    public Actor actor() {
        return _actor;
    }

    /**
     * 返回 parent ActorRef。
     *
     * @return 负责监督该 cell 的父级引用
     */
    public InternalActorRef parent() {
        return parent;
    }

    /**
     * 返回当前不可变 children 状态快照。
     *
     * <p>快照通过 volatile read 获取。修改方构造新容器后使用 CAS 发布。</p>
     *
     * @return 当前 children 状态
     */
    public ChildrenContainer childrenRefs() {
        return (ChildrenContainer) childrenRefsHandle.getVolatile(this);
    }

    private boolean swapChildrenRefs(ChildrenContainer oldValue, ChildrenContainer newValue) {
        return childrenRefsHandle.compareAndSet(this, oldValue, newValue);
    }

    void reserveChild(String name) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (swapChildrenRefs(current, current.reserve(name))) return;
        }
    }

    private void unreserveChild(String name) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (swapChildrenRefs(current, current.unreserve(name))) return;
        }
    }

    private void supervise(ActorRef child, boolean async) {
        if (childrenRefs().isTerminating()) return;
        ChildRestartStats childStats = initChild(child);
        if (childStats != null) {
            handleSupervise(child, async);
            publishLifecycle("now supervising " + child);
        } else {
            system.eventStream().publish(new Error(self.path().toString(), actorClass(),
                    "received Supervise from unregistered child " + child + ", this will not end well"));
        }
    }

    ChildRestartStats initChild(ActorRef child) {
        if (!(child instanceof ActorRefWithCell)) return null;
        String name = child.path().name();
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (current.isTerminating()) return null;
            var existing = current.getByName(name);
            if (existing instanceof ChildRestartStats stats && stats.child().equals(child)) return stats;
            if (existing != ChildNameReserved.INSTANCE) return null;
            ChildRestartStats childStats = new ChildRestartStats(child);
            if (swapChildrenRefs(current, current.add(name, childStats))) return childStats;
        }
    }

    @Override
    public Optional<ChildStats> getChildByName(String name) {
        return Optional.ofNullable(childrenRefs().getByName(name));
    }

    @Override
    public InternalActorRef getSingleChild(String name) {
        int separator = name.indexOf('#');
        String childName = separator < 0 ? name : name.substring(0, separator);
        int uid = separator < 0 ? ActorPath.UndefinedUid : Integer.parseInt(name.substring(separator + 1));
        if (childrenRefs().getByName(childName) instanceof ChildRestartStats stats
                && (uid == ActorPath.UndefinedUid || uid == stats.uid())) {
            return (InternalActorRef) stats.child();
        }
        return Nobody.INSTANCE;
    }

    /**
     * 处理 parent 对 child 的监督注册。
     *
     * @param child 已注册的 child
     * @param async 是否异步确认监督关系
     */
    protected void handleSupervise(ActorRef child, boolean async) {
        if (async && child instanceof RepointableActorRef repointable) repointable.point(true);
    }

    private ChildrenContainer.SuspendReason removeChildAndGetStateChange(ActorRef child) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            boolean wasWaitingForChild = current instanceof ChildrenContainer.TerminatingChildrenContainer terminating
                    && terminating.isWaitingFor(child);
            ChildrenContainer.SuspendReason reason = current instanceof ChildrenContainer.TerminatingChildrenContainer terminating
                    ? terminating.reason() : null;
            ChildrenContainer updated = current.remove(child);
            if (swapChildrenRefs(current, updated)) {
                boolean stillWaiting = updated instanceof ChildrenContainer.TerminatingChildrenContainer terminating
                        && terminating.isTerminatingChildren();
                return wasWaitingForChild && !stillWaiting ? reason : null;
            }
        }
    }

    private void watchedActorTerminated(DeathWatchNotification notification) {
        Optional<Object> customMessage = watching.remove(notification.actor);
        if (customMessage != null && !childrenRefs().isTerminating()) {
            sendMessage(new Terminated(notification.actor, notification.existenceConfirmed, notification.addressTerminated),
                    notification.actor);
            terminatedQueued.putIfAbsent(notification.actor, customMessage);
        }
        if (childrenRefs().getByRef(notification.actor) != null) handleChildTerminated(notification.actor);
    }

    protected void receivedTerminated(Terminated terminated) throws Exception {
        Optional<Object> customMessage = terminatedQueued.remove(terminated.actor());
        if (customMessage == null) return;
        if (customMessage.isPresent()) {
            Object message = customMessage.get();
            currentMessage = new Envelope(message == NullCustomWatchMessage ? null : message, currentMessage.sender());
            receiveMessage(message == NullCustomWatchMessage ? null : message);
        } else {
            receiveMessage(terminated);
        }
    }

    private void addWatcher(ActorRef watchee, ActorRef watcher) {
        boolean watcheeSelf = watchee.equals(self);
        boolean watcherSelf = watcher.equals(self);
        if (watcheeSelf && !watcherSelf) {
            if (!watchedBy.contains(watcher)) {
                Set<ActorRef> updated = new HashSet<>(watchedBy);
                updated.add(watcher);
                watchedBy = Set.copyOf(updated);
                publishLifecycle("now watched by " + watcher);
            }
        } else if (!watcheeSelf && watcherSelf) {
            watch(watchee);
        } else {
            system.eventStream().publish(new Warning(self.path().toString(), actorClass(),
                    "BUG: illegal Watch(" + watchee + "," + watcher + ") for " + self));
        }
    }

    private void remWatcher(ActorRef watchee, ActorRef watcher) {
        boolean watcheeSelf = watchee.equals(self);
        boolean watcherSelf = watcher.equals(self);
        if (watcheeSelf && !watcherSelf) {
            if (watchedBy.contains(watcher)) {
                Set<ActorRef> updated = new HashSet<>(watchedBy);
                updated.remove(watcher);
                watchedBy = Set.copyOf(updated);
                publishLifecycle("no longer watched by " + watcher);
            }
        } else if (!watcheeSelf && watcherSelf) {
            unwatch(watchee);
        } else {
            system.eventStream().publish(new Warning(self.path().toString(), actorClass(),
                    "BUG: illegal Unwatch(" + watchee + "," + watcher + ") for " + self));
        }
    }

    private void sendTerminated(ActorRef watcher) {
        if (watcher instanceof InternalActorRef internalWatcher) {
            internalWatcher.sendSystemMessage(
                    new DeathWatchNotification(self, true, false));
        }
    }

    /**
     * 返回创建或重建此 Actor 使用的 Props。
     *
     * @return 此 cell 的 Props
     */
    public Props props() {
        return _props;
    }

    /**
     * 返回调度此 cell mailbox 的 Dispatcher。
     *
     * @return 此 cell 的 Dispatcher
     */
    @Override
    public MessageDispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * 处理系统消息。系统消息使用独立队列，优先于普通消息执行。
     * Create、Recreate、Terminate 形成生命周期主线，Watch 和 DeathWatchNotification
     * 形成 DeathWatch 主线。
     *
     * @param message 待处理的 system message
     */
    public final void systemInvoke(SystemMessage message) {
        invokeAll(message, calculateState());
    }

    private void invokeAll(SystemMessage messages, int currentState) {
        while (messages != null) {
            SystemMessage message = messages;
            SystemMessage rest = message.next();
            message.unlink();
            try {
                if (shouldStash(message, currentState)) stash(message);
                else invokeSystemMessage(message);
            } catch (Throwable error) {
                handleNonFatalOrInterruptedException(error);
            }
            int newState = calculateState();
            SystemMessage todo = rest;
            if (newState < currentState) {
                SystemMessage unstashed = unstashAll();
                while (unstashed != null) {
                    SystemMessage next = unstashed.next();
                    unstashed.next(todo);
                    todo = unstashed;
                    unstashed = next;
                }
            }
            if (isTerminated()) {
                sendAllToDeadLetters(todo);
                return;
            }
            messages = todo;
            currentState = newState;
        }
    }

    private void invokeSystemMessage(SystemMessage message) {
        if (message instanceof Create create) create(create.failure);
        else if (message instanceof Recreate recreate) faultRecreate(recreate.cause);
        else if (message instanceof Terminate) terminate();
        else if (message instanceof Suspend) faultSuspend();
        else if (message instanceof Resume resume) faultResume(resume.causedByFailure);
        else if (message instanceof Watch watch) addWatcher(watch.watchee, watch.watcher);
        else if (message instanceof Unwatch unwatch) remWatcher(unwatch.watchee, unwatch.watcher);
        else if (message instanceof Failed failed) handleFailure(failed);
        else if (message instanceof DeathWatchNotification notification)
            watchedActorTerminated(notification);
        else if (message instanceof Supervise supervise) supervise(supervise.child, supervise.async);
        else if (message == NoMessage.INSTANCE) {
            return;
        }
    }

    private int calculateState() {
        if (childrenRefs() instanceof ChildrenContainer.TerminatingChildrenContainer terminating
                && terminating.reason() instanceof ChildrenContainer.WaitingForChildren) {
            return SuspendedWaitForChildrenState;
        }
        return mailbox().isSuspended() ? SuspendedState : DefaultState;
    }

    private boolean shouldStash(SystemMessage message, int state) {
        if (state == SuspendedWaitForChildrenState) return message instanceof StashWhenWaitingForChildren;
        if (state == SuspendedState) return message instanceof StashWhenFailed;
        return false;
    }

    protected void stash(SystemMessage message) {
        assert message.unlinked();
        message.next(sysmsgStash);
        sysmsgStash = message;
    }

    private SystemMessage unstashAll() {
        SystemMessage unstashed = sysmsgStash;
        sysmsgStash = null;
        return unstashed;
    }

    private void sendAllToDeadLetters(SystemMessage messages) {
        while (messages != null) {
            SystemMessage message = messages;
            messages = message.next();
            message.unlink();
            system.deadLetters().tell(message);
        }
    }

    /**
     * 处理 Create 消息并运行 Actor 构造及 preStart 生命周期。
     */
    private void create(Optional<ActorInitializationException> failure) {
        if (failure.isPresent()) throw failure.get();
        try {
            Actor created = newActor();
            created.aroundPreStart();
            checkReceiveTimeout(true);
            publishLifecycle("started (" + created + ")");
        } catch (InterruptedException error) {
            setFailedFatally();
            failActor();
            Thread.currentThread().interrupt();
            throw new ActorInitializationException(self, "interruption during creation", error);
        } catch (Throwable error) {
            rethrowIfFatal(error);
            if (actor() != null) setFailed(actor().self);
            failActor();
            if (error instanceof InstantiationException instantiation) {
                throw new ActorInitializationException(self,
                        "exception during creation, this problem is likely to occur because the class of the Actor you tried to create is either, "
                                + "a non-static inner class (in which case make sure it is a static inner class or use Props(new ...) or Props(new Creator ...)) "
                                + "or is missing an appropriate, reachable no-args constructor.",
                        instantiation.getCause());
            }
            Throwable rootCause = rootCauseOf(error);
            throw new ActorInitializationException(self,
                    "exception during creation, root cause message: ["
                            + (rootCause.getMessage() == null ? "No message available" : rootCause.getMessage()) + "]",
                    error);
        }
    }

    private void failActor() {
        if (actor() != null) {
            clearActorFields();
            _actor = null;
        }
    }

    private void clearActorFields() {
        currentMessage = null;
        behaviorStack.clear();
    }

    private static Throwable rootCauseOf(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * 创建 Actor 实例并建立初始 behavior。
     *
     * <p>先清空旧 behavior，再调用 Props.newActor；只有 constructor 未调用 become 时才压入
     * receive。restart 会复用这条路径，所以 Actor 实例会更换，而 ActorRef 和 ActorCell 不变。</p>
     *
     * @return 新创建的 Actor 实例
     * @throws Exception Creator 或反射创建 Actor 时抛出的异常
     */
    protected Actor newActor() throws Exception {
        LinkedList<ActorContext> stack = contextStack.get();
        stack.addFirst(this);
        try {
            behaviorStack.clear();
            Actor instance = props().newActor();
            if (instance == null)
                throw new ActorInitializationException("Actor instance passed to actorOf can't be null");
            if (behaviorStack.isEmpty()) behaviorStack.push(instance.receive());
            _actor = instance;
            return instance;
        } finally {
            if (!stack.isEmpty() && stack.peekFirst() == null) stack.removeFirst();
            if (!stack.isEmpty()) stack.removeFirst();
        }
    }

    /**
     * 在 mailbox 执行线程中调用当前 receive behavior。
     *
     * @param messageHandle 待处理的消息及其 sender
     */
    public void invoke(Envelope messageHandle) {
        Object message = messageHandle.message();
        int timeoutBeforeReceive = cancelReceiveTimeoutIfNeeded(message);
        try {
            currentMessage = messageHandle;
            if (message instanceof AutoReceivedMessage) {
                autoReceiveMessage(messageHandle);
            } else {
                receiveMessage(message);
            }
            currentMessage = null;
        } catch (Throwable error) {
            handleNonFatalOrInterruptedException(error);
        } finally {
            checkReceiveTimeoutIfNeeded(message, timeoutBeforeReceive);
        }
    }

    public void autoReceiveMessage(Envelope messageHandle) throws Exception {
        if (system.debugAutoReceive()) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(),
                    "received AutoReceiveMessage " + messageHandle));
        }
        Object message = messageHandle.message();
        if (message instanceof Terminated terminated) receivedTerminated(terminated);
        else if (message == Kill.getInstance()) throw new ActorKilledException("Kill");
        else if (message == PoisonPill.getInstance()) self.stop();
        else if (message instanceof ActorSelectionMessage selection) {
            if (selection.elements().isEmpty()) {
                invoke(Envelope.apply(selection.message(), sender(), system));
            } else {
                ActorSelection.deliverSelection(self, sender(), selection);
            }
        } else if (message instanceof Identify identify) {
            sender().tell(new ActorIdentity(identify.messageId(), Optional.of(self)), self);
        } else {
            throw new RuntimeException("Unexpected message for autoreceive: " + message);
        }
    }

    /**
     * 返回当前 receive timeout；未设置或已取消时返回 {@code null}。
     *
     * @return 当前 receive timeout；未设置时为 {@code null}
     */
    @Override
    public final Duration receiveTimeout() {
        ReceiveTimeoutState data = receiveTimeoutData;
        return data == null ? null : data.timeout;
    }

    /**
     * 设置静默后投递 {@link ReceiveTimeout} 的时长。
     *
     * @param timeout 静默时长
     */
    @Override
    public final void setReceiveTimeout(Duration timeout) {
        setReceiveTimeoutInternal(timeout);
    }

    /**
     * 取消 receive timeout。
     */
    @Override
    public void cancelReceiveTimeout() {
        setReceiveTimeoutInternal(null);
    }

    private void setReceiveTimeoutInternal(Duration timeout) {
        ReceiveTimeoutState data = receiveTimeoutData;
        if (data == null) {
            receiveTimeoutData = new ReceiveTimeoutState(timeout, emptyCancellable, 1);
        } else {
            data.timeout = timeout;
            data.version++;
        }
    }

    private int cancelReceiveTimeoutIfNeeded(Object message) {
        ReceiveTimeoutState data = receiveTimeoutData;
        if (data != null && !(message instanceof NotInfluenceReceiveTimeout)) cancelReceiveTimeoutTask();
        return receiveTimeoutVersion();
    }

    private void checkReceiveTimeoutIfNeeded(Object message, int timeoutBeforeReceive) {
        boolean timeoutChanged = receiveTimeoutVersion() != timeoutBeforeReceive;
        if (receiveTimeoutData != null || timeoutChanged) {
            checkReceiveTimeout(!(message instanceof NotInfluenceReceiveTimeout) || timeoutChanged);
        }
    }

    public final void checkReceiveTimeout(boolean reschedule) {
        ReceiveTimeoutState data = receiveTimeoutData;
        if (data == null) return;
        if (data.timeout == null) {
            cancelReceiveTimeoutTask();
        } else if (reschedule || data.task == emptyCancellable) {
            data.task.cancel();
            data.task = system.scheduler().scheduleOnce(data.timeout, self,
                    new ReceiveTimeout(data.timeout), dispatcher, ActorRef.noSender);
            data.version++;
        }
    }

    private int receiveTimeoutVersion() {
        ReceiveTimeoutState data = receiveTimeoutData;
        return data == null ? 0 : data.version;
    }

    public void cancelReceiveTimeoutTask() {
        ReceiveTimeoutState data = receiveTimeoutData;
        if (data != null && data.task != emptyCancellable) {
            data.task.cancel();
            data.task = emptyCancellable;
            data.version++;
        }
    }

    private void handleNonFatalOrInterruptedException(Throwable error) {
        rethrowIfFatal(error);
        handleInvokeFailure(error);
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
    }

    @SuppressWarnings("removal")
    static void rethrowIfFatal(Throwable error) {
        if (error instanceof VirtualMachineError || error instanceof ThreadDeath || error instanceof LinkageError) {
            throw (java.lang.Error) error;
        }
    }

    private static final class ReceiveTimeoutState {
        private Duration timeout;
        private Cancellable task;
        private int version;

        private ReceiveTimeoutState(Duration timeout, Cancellable task, int version) {
            this.timeout = timeout;
            this.task = task;
            this.version = version;
        }
    }

    private void handleInvokeFailure(Throwable error) {
        handleInvokeFailure(Set.of(), error);
    }

    @InternalStableApi
    @Override
    public final void handleInvokeFailure(Iterable<ActorRef> childrenNotToSuspend, Throwable error) {
        try {
            suspendNonRecursive();
            Set<ActorRef> skip = new HashSet<>();
            childrenNotToSuspend.forEach(skip::add);
            if (currentMessage != null && currentMessage.message() instanceof Failed failed
                    && !isFailed()) {
                setFailed(failed.child);
                skip.add(failed.child);
            } else if (!isFailed()) {
                setFailed(self);
            }
            suspendChildren(skip);
            Throwable failure = error instanceof InterruptedException
                    ? new ActorInterruptedException(error) : error;
            parent.sendSystemMessage(new Failed(self, failure, self.path().uid()));
        } catch (Throwable e) {
            rethrowIfFatal(e);
            publishError(e, "emergency stop: exception in failure handling for " + error.getClass()
                    + io.github.espen7.tapio.event.Logging.stackTraceFor(error));
            try {
                for (ActorRef child : childrenRefs().children()) stop(child);
            } finally {
                finishTerminate();
            }
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private sealed interface FailedInfo permits NoFailedInfo, FailedRef, FailedFatally {
    }

    private enum NoFailedInfo implements FailedInfo {
        INSTANCE
    }

    private record FailedRef(ActorRef ref) implements FailedInfo {
    }

    private enum FailedFatally implements FailedInfo {
        INSTANCE
    }

    private boolean isFailed() {
        return _failed instanceof FailedRef;
    }

    private boolean isFailedFatally() {
        return _failed == FailedFatally.INSTANCE;
    }

    private ActorRef perpetrator() {
        return _failed instanceof FailedRef failed ? failed.ref() : null;
    }

    protected void setFailed(ActorRef perpetrator) {
        if (!isFailedFatally()) _failed = new FailedRef(perpetrator);
    }

    protected void setFailedFatally() {
        _failed = FailedFatally.INSTANCE;
    }

    private void clearFailed() {
        if (isFailed()) _failed = NoFailedInfo.INSTANCE;
    }

    private void suspendNonRecursive() {
        dispatcher().suspend(this);
    }

    private void resumeNonRecursive() {
        dispatcher().resume(this);
    }

    private void suspendChildren(Set<ActorRef> exceptFor) {
        for (ActorRef child : childrenRefs().children()) {
            if (!exceptFor.contains(child) && child instanceof InternalActorRef internalChild) {
                internalChild.sendSystemMessage(new Suspend());
            }
        }
    }

    private void resumeChildren(Throwable causedByFailure, ActorRef perpetrator) {
        for (ActorRef child : childrenRefs().children()) {
            if (child instanceof InternalActorRef internalChild) {
                Throwable cause = child.equals(perpetrator) ? causedByFailure : null;
                internalChild.sendSystemMessage(new Resume(cause));
            }
        }
    }

    private void faultSuspend() {
        suspendNonRecursive();
        suspendChildren(Set.of());
    }

    private void faultResume(Throwable causedByFailure) {
        if (actor() == null) {
            publishError(causedByFailure, "changing Resume into Create after " + causedByFailure);
            faultCreate();
        } else if (isFailedFatally() && causedByFailure != null) {
            publishError(causedByFailure, "changing Resume into Restart after " + causedByFailure);
            faultRecreate(causedByFailure);
        } else {
            ActorRef perpetrator = perpetrator();
            try {
                resumeNonRecursive();
            } finally {
                if (causedByFailure != null) clearFailed();
            }
            resumeChildren(causedByFailure, perpetrator);
        }
    }

    private void faultCreate() {
        cancelReceiveTimeout();
        cancelReceiveTimeoutTask();
        for (ActorRef child : childrenRefs().children()) stop(child);
        if (!setChildrenTerminationReason(new ChildrenContainer.Creation())) finishCreate();
    }

    private void finishCreate() {
        try {
            resumeNonRecursive();
        } finally {
            clearFailed();
        }
        try {
            create(Optional.empty());
        } catch (Throwable error) {
            rethrowIfFatal(error);
            handleInvokeFailure(error);
        }
    }

    private void faultRecreate(Throwable cause) {
        if (actor() == null) {
            publishError(cause, "changing Recreate into Create after " + cause);
            faultCreate();
        } else if (childrenRefs().isNormal()) {
            Actor failedActor = actor();
            publishLifecycle("restarting");
            try {
                if (!isFailedFatally()) {
                    failedActor.aroundPreRestart(cause, currentMessage == null
                            ? Optional.empty() : Optional.ofNullable(currentMessage.message()));
                }
            } catch (Throwable error) {
                rethrowIfFatal(error);
                PreRestartException exception = new PreRestartException(self, error, cause,
                        currentMessage == null ? null : currentMessage.message());
                publishError(exception, error.getMessage());
                if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            } finally {
                clearActorFields();
            }
            if (!setChildrenTerminationReason(new ChildrenContainer.Recreation(cause))) finishRecreate(cause);
        } else {
            faultResume(null);
        }
    }

    private boolean setChildrenTerminationReason(ChildrenContainer.SuspendReason reason) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (!(current instanceof ChildrenContainer.TerminatingChildrenContainer terminating)
                    || !terminating.isTerminatingChildren()) return false;
            ChildrenContainer updated = terminating.withReason(reason);
            if (swapChildrenRefs(current, updated)) return true;
        }
    }

    private void finishRecreate(Throwable cause) {
        List<ActorRef> survivors = childrenRefs().children();
        try {
            try {
                resumeNonRecursive();
            } finally {
                clearFailed();
            }
            Actor freshActor = newActor();
            freshActor.aroundPostRestart(cause);
            checkReceiveTimeout(true);
            publishLifecycle("restarted");
            for (ActorRef child : survivors) {
                if (child instanceof InternalActorRef internalChild) {
                    try {
                        internalChild.restart(cause);
                    } catch (Throwable error) {
                        rethrowIfFatal(error);
                        publishError(error, "restarting " + child);
                        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (Throwable error) {
            rethrowIfFatal(error);
            setFailedFatally();
            clearActorFields();
            handleInvokeFailure(new HashSet<>(survivors), new PostRestartException(self, error, cause));
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private void terminate() {
        cancelReceiveTimeout();
        cancelReceiveTimeoutTask();
        unwatchWatchedActors();
        for (ActorRef child : childrenRefs().children()) stop(child);
        boolean wasTerminating = childrenRefs().isTerminating();
        if (setChildrenTerminationReason(ChildrenContainer.Termination.INSTANCE)) {
            if (!wasTerminating) {
                suspendNonRecursive();
                setFailed(self);
                publishLifecycle("stopping");
            }
        } else {
            setTerminated();
            finishTerminate();
        }
    }

    private void handleFailure(Failed failed) {
        currentMessage = Envelope.apply(failed, failed.child, system);
        ChildRestartStats child = childrenRefs().getByRef(failed.child);
        if (child == null) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(),
                    "dropping Failed(" + failed.cause + ") from unknown child " + failed.child));
            return;
        }
        if (child.uid() != failed.uid) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(),
                    "dropping Failed(" + failed.cause + ") from old child " + failed.child
                            + " (uid=" + child.uid() + " != " + failed.uid + ")"));
            return;
        }
        if (!actor().supervisorStrategy().handleFailure(this, failed.child, failed.cause, child, childrenRefs().stats())) {
            ActorCell.<RuntimeException>throwUnchecked(failed.cause);
        }
    }

    @SuppressWarnings("unchecked")
    @InternalApi
    public static <E extends Throwable> void throwUnchecked(Throwable error) throws E {
        throw (E) error;
    }

    private void handleChildTerminated(ActorRef child) {
        ChildrenContainer.SuspendReason stateChange = removeChildAndGetStateChange(child);
        if (actor() != null) {
            try {
                actor().supervisorStrategy().handleChildTerminated(this, child, childrenRefs().children());
            } catch (Throwable error) {
                rethrowIfFatal(error);
                publishError(error, "handleChildTerminated failed");
                handleInvokeFailure(error);
            }
        }
        if (stateChange instanceof ChildrenContainer.Recreation recreation) finishRecreate(recreation.cause());
        else if (stateChange instanceof ChildrenContainer.Creation) finishCreate();
        else if (stateChange == ChildrenContainer.Termination.INSTANCE) finishTerminate();
    }

    private void setTerminated() {
        childrenRefsHandle.setVolatile(this, ChildrenContainer.terminated());
    }

    private void finishTerminate() {
        Actor terminatedActor = actor();
        try {
            if (terminatedActor != null) terminatedActor.aroundPostStop();
        } catch (Throwable error) {
            rethrowIfFatal(error);
            publishError(error, error.getMessage());
        } finally {
            try {
                dispatcher.detach(this);
            } finally {
                try {
                    try {
                        try {
                            parent.sendSystemMessage(new DeathWatchNotification(self, true, false));
                        } finally {
                            for (ActorRef watcher : watchedBy) {
                                if (!watcher.equals(parent)) sendTerminated(watcher);
                            }
                        }
                    } finally {
                        unwatchWatchedActors();
                        publishLifecycle("stopped");
                    }
                } finally {
                    watchedBy = Set.of();
                    clearActorFields();
                    clearFieldsForTermination();
                }
            }
        }
    }

    protected final void clearFieldsForTermination() {
        unstashAll();
        _props = terminatedProps;
        _actor = null;
    }

    private void unwatchWatchedActors() {
        if (!watching.isEmpty()) {
            try {
                for (ActorRef watchee : watching.keySet()) {
                    if (watchee instanceof InternalActorRef internalWatchee) {
                        internalWatchee.sendSystemMessage(new Unwatch(watchee, self));
                    }
                }
            } finally {
                watching.clear();
                terminatedQueued.clear();
            }
        }
    }

    private void publishLifecycle(String message) {
        if (system.debugLifecycle()) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(), message));
        }
    }

    private void publishError(Throwable cause, String message) {
        system.eventStream().publish(new Error(self.path().toString(), actorClass(), String.valueOf(message), cause));
    }

    private Class<?> actorClass() {
        return actor() == null ? ActorCell.class : actor().getClass();
    }

    /**
     * @return 此 ActorCell 所属的 ActorSystem
     */
    @Override
    public ActorSystemImpl system() {
        return system;
    }

    @Override
    public final ActorSystemImpl systemImpl() {
        return system;
    }

    @Override
    public final boolean isLocal() {
        return true;
    }

    /**
     * 返回当前 Envelope 的 sender。
     *
     * @return 当前消息 sender；Actor 正在处理消息之外时返回 dead letters
     */
    @Override
    public ActorRef sender() {
        return currentMessage == null ? system.deadLetters() : currentMessage.sender();
    }

    /** INTERNAL API：返回当前正在处理的 Envelope。 */
    Envelope currentMessage() {
        return currentMessage;
    }

    /** INTERNAL API：设置当前正在处理的 Envelope。 */
    void setCurrentMessage(Envelope message) {
        currentMessage = message;
    }

    /** INTERNAL API：确保来自 stash 的 {@code Terminated} 消息可被再次投递。 */
    public void terminatedQueuedFor(ActorRef actor, Optional<Object> customMessage) {
        terminatedQueued.putIfAbsent(actor, customMessage);
    }

    /**
     * 使用自动生成的名称创建 child。
     */
    @Override
    public ActorRef actorOf(Props props) {
        return makeChild(props, randomName(), false, false);
    }

    /**
     * 在当前 Actor 下创建 child，并先通过 ChildrenContainer 预留 name。
     *
     * @param props child 的 Actor 创建配置
     * @param name  parent 下唯一的 child name
     * @return 新 child 的引用
     */
    @Override
    public ActorRef actorOf(Props props, String name) {
        return makeChild(props, checkName(name), false, false);
    }

    @Override
    public ActorSelection actorSelection(String path) {
        return ActorSelection.apply(system.provider(), self, path);
    }

    @Override
    public ActorSelection actorSelection(ActorPath path) {
        return ActorSelection.apply(system.provider(), path);
    }

    ActorRef attachChild(Props props, boolean systemService) {
        return makeChild(props, randomName(), true, systemService);
    }

    /**
     * 在当前 Actor 下挂载 child。
     *
     * <p>方法先预留名称，再交给 ActorRefProvider 创建引用。创建失败时会释放名称，
     * 以便调用者重试同名 child。</p>
     *
     * @param props child 的 Actor 创建配置
     * @param name parent 下唯一的 child name
     * @param systemService 是否为系统服务 child
     * @return 新 child 的引用
     */
    public ActorRef attachChild(Props props, String name, boolean systemService) {
        return makeChild(props, checkName(name), true, systemService);
    }

    private String randomName() {
        return Helpers.base64((long) nextNameHandle.getAndAdd(this, 1L), new StringBuilder("$"));
    }

    private static String checkName(String name) {
        if (name == null) throw new InvalidActorNameException("actor name must not be null");
        if (name.isEmpty()) throw new InvalidActorNameException("actor name must not be empty");
        ActorPath.validatePathElement(name);
        return name;
    }

    private static int newUid() {
        int uid;
        do {
            uid = ThreadLocalRandom.current().nextInt();
        } while (uid == ActorPath.UndefinedUid);
        return uid;
    }

    private ActorRef makeChild(Props props, String name, boolean async, boolean systemService) {
        if (childrenRefs().isTerminating()) {
            throw new IllegalStateException("cannot create children while terminating or terminated");
        }
        reserveChild(name);
        InternalActorRef child;
        try {
            ActorPath path = new ActorPath(self.path(), name, newUid());
            child = system.provider().actorOf(system, props, self, path, systemService, Optional.empty(), true, async);
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                unreserveChild(name);
                /** 按 Java 惯例，在抛出前清除中断标志 */
                Thread.interrupted();
                throw new RuntimeException(failure);
            }
            rethrowIfFatal(failure);
            unreserveChild(name);
            if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
            if (failure instanceof java.lang.Error javaError) throw javaError;
            throw new RuntimeException(failure);
        }
        if (mailbox() != null) {
            int suspendCount = mailbox().suspendCount();
            for (int i = 0; i < suspendCount; i++) {
                child.suspend();
            }
        }
        initChild(child);
        child.start();
        return child;
    }

    /**
     * 异步停止 Actor；若目标是当前 child，会先更新 ChildrenContainer。
     */
    @Override
    public void stop(ActorRef actor) {
        if (!(actor instanceof RepointableActorRef repointable) || repointable.isStarted()) {
            for (; ; ) {
                ChildrenContainer current = childrenRefs();
                if (current.getByRef(actor) == null || swapChildrenRefs(current, current.shallDie(actor))) break;
            }
        }
        ((InternalActorRef) actor).stop();
    }

    @Override
    public boolean isWatching(ActorRef ref) {
        return watching.containsKey(ref);
    }

    /**
     * 开始观察目标 Actor 的终止状态。
     */
    @Override
    public ActorRef watch(ActorRef actor) {
        return watch(actor, Optional.empty());
    }

    /**
     * 观察目标 Actor，并在其终止时投递自定义消息。
     */
    @Override
    public ActorRef watchWith(ActorRef actor, Object message) {
        return watch(actor, Optional.of(message == null ? NullCustomWatchMessage : message));
    }

    private ActorRef watch(ActorRef actor, Optional<Object> message) {
        if (!(actor instanceof InternalActorRef internalActor)) {
            throw new IllegalArgumentException("ActorRef is not internal: " + actor);
        }
        if (!actor.equals(self)) {
            Optional<Object> previous = watching.get(actor);
            if (previous == null) {
                internalActor.sendSystemMessage(new Watch(internalActor, self));
                watching.put(actor, message);
            } else {
                checkWatchingSame(actor, previous, message);
            }
        }
        return actor;
    }

    private void checkWatchingSame(ActorRef actor, Optional<Object> previous, Optional<Object> message) {
        if (!previous.equals(message)) {
            throw new IllegalStateException("Watch(" + self + ", " + actor
                    + ") termination message was not overwritten from [" + formatWatchMessage(previous) + "] to ["
                    + formatWatchMessage(message)
                    + "]. If this was intended, unwatch first before using `watch` / `watchWith` with another message.");
        }
    }

    private static String formatWatchMessage(Optional<Object> message) {
        if (message.isEmpty()) return "None";
        Object value = message.get();
        return "Some(" + (value == NullCustomWatchMessage ? null : value) + ")";
    }

    /**
     * 取消对此 Actor 的 DeathWatch 观察。
     */
    @Override
    public ActorRef unwatch(ActorRef actor) {
        if (!(actor instanceof InternalActorRef internalActor)) {
            throw new IllegalArgumentException("ActorRef is not internal: " + actor);
        }
        if (!actor.equals(self) && watching.remove(actor) != null) {
            internalActor.sendSystemMessage(new Unwatch(actor, self));
        }
        terminatedQueued.remove(actor);
        return actor;
    }

    /**
     * 将当前 behavior 替换为指定 behavior。
     */
    @Override
    public void become(PartialFunction<Object, Void> behavior) {
        become(behavior, true);
    }

    /**
     * 切换当前 behavior。
     *
     * @param behavior   新 behavior
     * @param discardOld 为 {@code true} 时替换栈顶；为 {@code false} 时将新 behavior 压栈
     */
    @Override
    public void become(PartialFunction<Object, Void> behavior, boolean discardOld) {
        if (discardOld && !behaviorStack.isEmpty()) behaviorStack.pop();
        behaviorStack.push(behavior);
    }

    /**
     * 弹出当前 behavior；栈清空后恢复 Actor 的初始 behavior。
     */
    @Override
    public void unbecome() {
        if (!behaviorStack.isEmpty()) behaviorStack.pop();
        if (behaviorStack.isEmpty()) behaviorStack.push(actor().receive());
    }

    /**
     * @return 当前 child ActorRef 的不可变快照
     */
    @Override
    public Iterable<ActorRef> children() {
        return childrenRefs().children();
    }

    @Override
    public Optional<ActorRef> child(String name) {
        return getChildByName(name).filter(ChildRestartStats.class::isInstance)
                .map(ChildRestartStats.class::cast).map(ChildRestartStats::child);
    }

    public final void receiveMessage(Object message) throws Exception {
        actor().aroundReceive(behaviorStack.peek(), message);
    }
}
