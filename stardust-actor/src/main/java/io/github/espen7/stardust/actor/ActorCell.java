package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.dispatch.Envelope;
import io.github.espen7.stardust.dispatch.Mailbox;
import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;
import io.github.espen7.stardust.event.Debug;
import io.github.espen7.stardust.event.Error;
import io.github.espen7.stardust.event.LogEvent;
import io.github.espen7.stardust.event.Warning;
import io.github.espen7.stardust.actor.dungeon.ChildNameReserved;
import io.github.espen7.stardust.actor.dungeon.ChildRestartStats;
import io.github.espen7.stardust.actor.dungeon.ChildrenContainer;

import java.util.ArrayDeque;
import java.util.Collection;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存 Actor 实例及其运行时状态。
 *
 * <p>ActorRef 是公开的地址，ActorCell 才拥有 Actor 实例、Mailbox、children、
 * watcher 和当前消息。所有用户消息都在 Dispatcher 调度的 Mailbox 线程中进入
 * {@link #invoke(Envelope)}，因此 Actor 的 receive 回调在同一时间只会有一个执行者。</p>
 */
public class ActorCell implements ActorContext {
    private static final VarHandle childrenRefsHandle;

    static {
        try {
            childrenRefsHandle = MethodHandles.lookup().findVarHandle(
                    ActorCell.class, "childrenRefs", ChildrenContainer.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    /** 所属 ActorSystem。 */
    private final ActorSystemImpl system;
    /** 此 cell 的 ActorRef。 */
    private final LocalActorRef self;
    /** 创建或重建 Actor 实例时使用的 Props。 */
    private final Props initialProps;
    /** 调度此 cell mailbox 的 Dispatcher。 */
    private final Dispatcher dispatcher;
    /** 此 Actor 的 parent 和 supervisor。 */
    private final ActorRef parent;
    private final Mailbox mailbox;
    /** children 状态快照通过 CAS 更新，供运行时并发读取。 */
    @SuppressWarnings("FieldMayBeFinal")
    private volatile ChildrenContainer childrenRefs = ChildrenContainer.empty();
    /** 当前 watch 本 Actor 的引用快照，由系统消息处理路径更新。 */
    private Set<ActorRef> watchedBy = Set.of();
    private final Set<ActorRef> watching = new HashSet<>();
    /** 保存当前 behavior 及其被压入的旧 behavior。 */
    private final ArrayDeque<AbstractActor.Receive> behaviorStack = new ArrayDeque<>();
    /** Actor 构造期间暂存 ActorContext。 */
    static final ThreadLocal<LinkedList<ActorContext>> contextStack = ThreadLocal.withInitial(LinkedList::new);
    private volatile Actor actor;
    /** 当前处理的 Envelope，供 ActorContext.sender() 读取。 */
    private volatile Envelope currentMessage;
    private volatile boolean terminated;
    private volatile Envelope failedMessage;
    private ActorRef failedPerpetrator;
    private boolean failedFatally;

    /**
     * 创建 ActorCell 并绑定所属 runtime 组件。
     *
     * @param system     所属 ActorSystem
     * @param self       此 cell 对应的本地 ActorRef
     * @param props      创建或重新创建 Actor 实例所用的 Props
     * @param dispatcher 调度此 cell mailbox 的 Dispatcher
     * @param parent     supervisor 和 parent ActorRef；root guardian 没有 parent
     */
    public ActorCell(ActorSystemImpl system, LocalActorRef self, Props props, Dispatcher dispatcher, ActorRef parent) {
        this.system = system;
        this.self = self;
        this.initialProps = props;
        this.dispatcher = dispatcher;
        this.parent = parent;
        this.mailbox = new Mailbox(new io.github.espen7.stardust.dispatch.UnboundedMailbox());
        mailbox.setActor(this, dispatcher);
    }

    /**
     * @return 此 ActorCell 的 Mailbox
     */
    public Mailbox mailbox() {
        return mailbox;
    }

    /**
     * @return 此 ActorCell 对应的 ActorRef
     */
    public LocalActorRef self() {
        return self;
    }

    /**
     * @return 当前 Actor 实例；Create 完成前可能为 {@code null}
     */
    public Actor actor() {
        return actor;
    }

    /**
     * @return parent ActorRef；root guardian 没有 parent
     */
    public ActorRef parent() {
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

    private void reserveChild(String name) {
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

    private ChildRestartStats initChild(ActorRef child) {
        if (!(child instanceof LocalActorRef localChild)) return null;
        String name = localChild.name();
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (current.isTerminating()) return null;
            if (current.getByName(name) != ChildNameReserved.INSTANCE) {
                return null;
            }
            ChildRestartStats childStats = new ChildRestartStats(child);
            if (swapChildrenRefs(current, current.add(name, childStats))) return childStats;
        }
    }

    protected void handleSupervise(ActorRef child, boolean async) {
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

    private void watchedActorTerminated(SystemMessage.DeathWatchNotification notification) {
        boolean wasWatching = watching.remove(notification.actor);
        if (wasWatching && !childrenRefs().isTerminating()) {
            sendMessage(new Terminated(notification.actor, notification.existenceConfirmed, notification.addressTerminated),
                    notification.actor);
        }
        if (childrenRefs().getByRef(notification.actor) != null) handleChildTerminated(notification.actor);
    }

    private void addWatcher(ActorRef watchee, ActorRef watcher) {
        boolean watcheeSelf = watchee.equals(self);
        boolean watcherSelf = watcher.equals(self);
        if (watcheeSelf && !watcherSelf) {
            if (terminated) {
                sendTerminated(watcher);
            } else if (!watchedBy.contains(watcher)) {
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
        if (watcher instanceof LocalActorRef localWatcher) {
            localWatcher.underlying().sendSystemMessage(
                    new SystemMessage.DeathWatchNotification(self, true, false));
        }
    }

    /**
     * @return 当此 ActorCell 已进入终止状态时返回 {@code true}
     */
    public boolean isTerminated() {
        return terminated;
    }

    /**
     * 初始化 Mailbox，并异步投递 Create 系统消息。
     */
    /**
     * 初始化 ActorCell，并向 Mailbox 放入 Create 系统消息。
     *
     * @param sendSupervise 是否向 parent 发送 Supervise 系统消息
     */
    public void init(boolean sendSupervise) {
        mailbox.systemEnqueue(self, new SystemMessage.Create(null));
        if (sendSupervise && parent != null) {
            ((LocalActorRef) parent).underlying().sendSystemMessage(new SystemMessage.Supervise(self, false));
        }
        dispatcher.registerForExecution(mailbox, false, true);
    }

    /**
     * 启动此 ActorCell。
     *
     * <p>当前实现由 {@link #init(boolean)} 中的 Create 系统消息启动 mailbox，
     * 此方法暂时保留上游入口但没有额外动作。</p>
     */
    public void start() {
    }


    /**
     * 将用户消息封装为 Envelope 并交给 Dispatcher 异步投递。
     *
     * @param message 用户消息，不允许为 {@code null}
     * @param sender  发送者引用；为 {@code null} 时转换成 dead letters 引用
     */
    public void sendMessage(Object message, ActorRef sender) {
        dispatcher.dispatch(this, Envelope.apply(message, sender, system));
    }

    /**
     * 将 runtime 控制消息投递到独立的 system message queue。
     */
    public void sendSystemMessage(SystemMessage message) {
        dispatcher.systemDispatch(this, message);
    }

    /**
     * 处理系统消息。系统消息使用独立队列，优先于普通消息执行。
     * Create、Recreate、Terminate 形成生命周期主线，Watch 和 DeathWatchNotification
     * 形成 DeathWatch 主线。
     */
    public void systemInvoke(SystemMessage message) {
        Envelope previousMessage = currentMessage;
        try {
            if (message instanceof SystemMessage.Create create) create(create.failure);
            else if (message instanceof SystemMessage.Recreate recreate) faultRecreate(recreate.cause);
            else if (message instanceof SystemMessage.Terminate) terminate();
            else if (message instanceof SystemMessage.Suspend) faultSuspend();
            else if (message instanceof SystemMessage.Resume resume) faultResume(resume.causedByFailure);
            else if (message instanceof SystemMessage.Watch watch) {
                addWatcher(watch.watchee, watch.watcher);
            } else if (message instanceof SystemMessage.Unwatch unwatch) {
                remWatcher(unwatch.watchee, unwatch.watcher);
            }
            else if (message instanceof SystemMessage.Failed failed) handleFailure(failed);
            else if (message instanceof SystemMessage.DeathWatchNotification notification)
                watchedActorTerminated(notification);
            else if (message instanceof SystemMessage.Supervise supervise) supervise(supervise.child, supervise.async);
        } catch (Throwable error) {
            rethrowIfFatal(error);
            handleInvokeFailure(error);
        } finally {
            currentMessage = previousMessage;
        }
    }

    /**
     * 处理 Create 消息并运行 Actor 构造及 preStart 生命周期。
     */
    private void create(Throwable failure) {
        if (failure != null) {
            handleInvokeFailure(failure);
            return;
        }
        try {
            newActor().preStart();
            publishLifecycle("started (" + actor + ")");
        } catch (Throwable error) {
            rethrowIfFatal(error);
            handleInvokeFailure(error);
        }
    }

    /**
     * 创建 Actor 实例并建立初始 behavior。
     *
     * <p>先清空旧 behavior，再调用 Props.newActor，最后把 receive 放入栈顶。
     * restart 会复用这条路径，所以 Actor 实例会更换，而 ActorRef 和 ActorCell 不变。</p>
     */
    protected Actor newActor() {
        LinkedList<ActorContext> stack = contextStack.get();
        stack.addFirst(this);
        try {
            behaviorStack.clear();
            Actor instance = initialProps.newActor();
            if (instance == null)
                throw new ActorInitializationException("Actor instance passed to actorOf can't be null");
            actor = instance;
            behaviorStack.push(instance.receive());
            return instance;
        } finally {
            if (!stack.isEmpty() && stack.peekFirst() == null) stack.removeFirst();
            if (!stack.isEmpty()) stack.removeFirst();
        }
    }

    /**
     * 处理一条用户消息。currentMessage 在回调前设置，在回调后清除，避免 sender
     * 泄漏到下一条消息。回调抛出的异常交给 SupervisorStrategy。
     */
    /**
     * 在 mailbox 执行线程中调用当前 receive behavior。
     *
     * @param messageHandle 待处理的消息及其 sender
     */
    public void invoke(Envelope messageHandle) {
        if (terminated) {
            system.deadLetters().tell(messageHandle.message(), messageHandle.sender());
            return;
        }
        currentMessage = messageHandle;
        try {
            behaviorStack.peek().apply(messageHandle.message(), this);
        } catch (Throwable error) {
            rethrowIfFatal(error);
            handleInvokeFailure(error);
        } finally {
            currentMessage = null;
        }
    }

    @SuppressWarnings("removal")
    private static void rethrowIfFatal(Throwable error) {
        if (error instanceof VirtualMachineError || error instanceof ThreadDeath || error instanceof LinkageError) {
            throw (java.lang.Error) error;
        }
    }

    private void handleInvokeFailure(Throwable error) {
        handleInvokeFailure(Set.of(), error);
    }

    private void handleInvokeFailure(Set<ActorRef> childrenNotToSuspend, Throwable error) {
        failedMessage = currentMessage;
        mailbox.suspend();
        Set<ActorRef> skip = new HashSet<>(childrenNotToSuspend);
        if (currentMessage != null && currentMessage.message() instanceof SystemMessage.Failed failed
                && failedPerpetrator == null) {
            setFailed(failed.child);
            skip.add(failed.child);
        } else if (failedPerpetrator == null) {
            setFailed(self);
        }
        suspendChildren(skip);
        if (parent instanceof LocalActorRef localParent) {
            localParent.underlying().sendSystemMessage(new SystemMessage.Failed(self, error, self.path().uid()));
        } else {
            sendSystemMessage(new SystemMessage.Terminate());
        }
    }

    private void setFailed(ActorRef perpetrator) {
        if (!failedFatally) failedPerpetrator = perpetrator;
    }

    private void clearFailed() {
        failedPerpetrator = null;
    }

    private void suspendChildren(Set<ActorRef> exceptFor) {
        for (ActorRef child : childrenRefs().children()) {
            if (!exceptFor.contains(child) && child instanceof LocalActorRef localChild) {
                localChild.underlying().sendSystemMessage(new SystemMessage.Suspend());
            }
        }
    }

    private void resumeChildren(Throwable causedByFailure, ActorRef perpetrator) {
        for (ActorRef child : childrenRefs().children()) {
            if (child instanceof LocalActorRef localChild) {
                Throwable cause = child.equals(perpetrator) ? causedByFailure : null;
                localChild.underlying().sendSystemMessage(new SystemMessage.Resume(cause));
            }
        }
    }

    private void faultSuspend() {
        mailbox.suspend();
        suspendChildren(Set.of());
    }

    private void faultResume(Throwable causedByFailure) {
        if (actor == null) {
            publishError(causedByFailure, "changing Resume into Create after " + causedByFailure);
            faultCreate();
        } else if (failedFatally && causedByFailure != null) {
            publishError(causedByFailure, "changing Resume into Restart after " + causedByFailure);
            faultRecreate(causedByFailure);
        } else {
            ActorRef perpetrator = failedPerpetrator;
            try {
                mailbox.resume();
            } finally {
                if (causedByFailure != null) clearFailed();
            }
            resumeChildren(causedByFailure, perpetrator);
        }
    }

    private void faultCreate() {
        for (ActorRef child : childrenRefs().children()) stop(child);
        if (!setChildrenTerminationReason(new ChildrenContainer.Creation())) finishCreate();
    }

    private void finishCreate() {
        try {
            mailbox.resume();
        } finally {
            clearFailed();
        }
        create(null);
    }

    private void faultRecreate(Throwable cause) {
        if (actor == null) {
            publishError(cause, "changing Recreate into Create after " + cause);
            faultCreate();
        } else if (childrenRefs().isNormal()) {
            Actor failedActor = actor;
            publishLifecycle("restarting");
            try {
                if (!failedFatally) {
                    failedActor.preRestart(cause, failedMessage == null ? null : failedMessage.message());
                }
            } catch (Throwable error) {
                rethrowIfFatal(error);
                publishError(error, error.getMessage());
            } finally {
                actor = null;
                behaviorStack.clear();
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
                mailbox.resume();
            } finally {
                clearFailed();
            }
            Actor freshActor = newActor();
            freshActor.postRestart(cause);
            publishLifecycle("restarted");
            for (ActorRef child : survivors) {
                if (child instanceof LocalActorRef localChild) {
                    localChild.underlying().sendSystemMessage(new SystemMessage.Recreate(cause));
                }
            }
        } catch (Throwable error) {
            rethrowIfFatal(error);
            failedFatally = true;
            actor = null;
            behaviorStack.clear();
            handleInvokeFailure(new HashSet<>(survivors), error);
        }
    }

    private void terminate() {
        if (terminated) return;
        unwatchWatchedActors();
        for (ActorRef child : childrenRefs().children()) stop(child);
        boolean wasTerminating = childrenRefs().isTerminating();
        if (setChildrenTerminationReason(ChildrenContainer.Termination.INSTANCE)) {
            if (!wasTerminating) {
                mailbox.suspend();
                setFailed(self);
                publishLifecycle("stopping");
            }
        } else {
            setTerminated();
            finishTerminate();
        }
    }

    private void handleFailure(SystemMessage.Failed failed) {
        currentMessage = Envelope.apply(failed, failed.child, system);
        ChildRestartStats child = childrenRefs().getByRef(failed.child);
        if (child == null) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(),
                    "dropping Failed(" + failed.cause + ") from unknown child " + failed.child));
            currentMessage = null;
            return;
        }
        if (child.uid() != failed.uid) {
            system.eventStream().publish(new Debug(self.path().toString(), actorClass(),
                    "dropping Failed(" + failed.cause + ") from old child " + failed.child
                            + " (uid=" + child.uid() + " != " + failed.uid + ")"));
            currentMessage = null;
            return;
        }
        if (!actor.supervisorStrategy().handleFailure(this, failed.child, failed.cause, child, childrenRefs().stats())) {
            ActorCell.<RuntimeException>throwUnchecked(failed.cause);
        }
        currentMessage = null;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwUnchecked(Throwable error) throws E {
        throw (E) error;
    }

    private void handleChildTerminated(ActorRef child) {
        ChildrenContainer.SuspendReason stateChange = removeChildAndGetStateChange(child);
        if (actor != null) {
            try {
                actor.supervisorStrategy().handleChildTerminated(this, child, childrenRefs().children());
            } catch (Throwable error) {
                rethrowIfFatal(error);
                publishError(error, "handleChildTerminated failed");
                handleInvokeFailure(error);
            }
        }
        if (stateChange instanceof ChildrenContainer.Recreation recreation) finishRecreate(recreation.cause());
        else if (stateChange instanceof ChildrenContainer.Creation) finishCreate();
        else if (stateChange == ChildrenContainer.Termination.INSTANCE) {
            setTerminated();
            finishTerminate();
        }
    }

    private void setTerminated() {
        terminated = true;
        childrenRefsHandle.setVolatile(this, ChildrenContainer.terminated());
    }

    private void finishTerminate() {
        Actor terminatedActor = actor;
        try {
            if (terminatedActor != null) terminatedActor.postStop();
        } catch (Throwable error) {
            rethrowIfFatal(error);
            publishError(error, error.getMessage());
        } finally {
            if (parent instanceof LocalActorRef localParent) {
                localParent.underlying().sendSystemMessage(
                        new SystemMessage.DeathWatchNotification(self, true, false));
            }
            for (ActorRef watcher : watchedBy) {
                if (!watcher.equals(parent)) sendTerminated(watcher);
            }
            watchedBy = Set.of();
            publishLifecycle("stopped");
            mailbox.close();
            mailbox.cleanUp();
            actor = null;
            behaviorStack.clear();
            if (parent == null) system.actorTerminated(self);
        }
    }

    private void unwatchWatchedActors() {
        for (ActorRef watchee : Set.copyOf(watching)) {
            if (watchee instanceof LocalActorRef localWatchee) {
                localWatchee.underlying().sendSystemMessage(new SystemMessage.Unwatch(watchee, self));
            }
        }
        watching.clear();
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
        return actor == null ? ActorCell.class : actor.getClass();
    }

    /**
     * @return 此 ActorCell 所属的 ActorSystem
     */
    @Override
    public ActorSystem system() {
        return system;
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

    /**
     * 使用基于当前时间的 name 创建 child。
     */
    @Override
    public ActorRef actorOf(Props props) {
        return actorOf(props, "actor-" + System.nanoTime());
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
        reserveChild(name);
        try {
            return system.provider().actorOf(props, name, self);
        } catch (RuntimeException failure) {
            unreserveChild(name);
            throw failure;
        }
    }

    /**
     * 异步停止 Actor；若目标是当前 child，会先更新 ChildrenContainer。
     */
    @Override
    public void stop(ActorRef actor) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (current.getByRef(actor) == null || swapChildrenRefs(current, current.shallDie(actor))) break;
        }
        actor.stop();
    }

    /**
     * 开始观察目标 Actor 的终止状态。
     */
    @Override
    public ActorRef watch(ActorRef actor) {
        if (actor.equals(self)) return actor;
        if (watching.add(actor)) {
            ((LocalActorRef) actor).underlying().sendSystemMessage(new SystemMessage.Watch(actor, self));
        }
        return actor;
    }

    /**
     * 取消对此 Actor 的 DeathWatch 观察。
     */
    @Override
    public ActorRef unwatch(ActorRef actor) {
        if (!actor.equals(self) && watching.remove(actor)) {
            ((LocalActorRef) actor).underlying().sendSystemMessage(new SystemMessage.Unwatch(actor, self));
        }
        return actor;
    }

    /**
     * 将当前 behavior 替换为指定 behavior。
     */
    @Override
    public void become(AbstractActor.Receive behavior) {
        become(behavior, true);
    }

    /**
     * 切换当前 behavior。
     *
     * @param behavior   新 behavior
     * @param discardOld 为 {@code true} 时替换栈顶；为 {@code false} 时将新 behavior 压栈
     */
    @Override
    public void become(AbstractActor.Receive behavior, boolean discardOld) {
        if (discardOld && !behaviorStack.isEmpty()) behaviorStack.pop();
        behaviorStack.push(behavior);
    }

    /**
     * 弹出当前 behavior；仅剩初始 behavior 时保持不变。
     */
    @Override
    public void unbecome() {
        if (behaviorStack.size() > 1) behaviorStack.pop();
    }

    /**
     * @return 当前 child ActorRef 的不可变快照
     */
    @Override
    public Collection<ActorRef> getChildren() {
        return childrenRefs().children();
    }
}
