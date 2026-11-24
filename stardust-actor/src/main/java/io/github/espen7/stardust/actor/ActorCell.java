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
import java.util.Set;

/**
 * Actor 的运行时状态容器，对应 Pekko 的 ActorCell。
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

    // 这些字段对应 Pekko ActorCell 的 system、self、props、dispatcher 和 parent。
    private final ActorSystemImpl system;
    private final LocalActorRef self;
    private final Props initialProps;
    private final Dispatcher dispatcher;
    private final ActorRef parent;
    private final Mailbox mailbox;
    // 不可变快照由 CAS 更新，允许 actor 外部安全读取 child 状态。
    @SuppressWarnings("FieldMayBeFinal")
    private volatile ChildrenContainer childrenRefs = ChildrenContainer.empty();
    // watchedBy 是不可变快照，只由本 ActorCell 的系统消息处理路径替换。
    private Set<ActorRef> watchedBy = Set.of();
    // behaviorStack 对应 become/unbecome 的当前行为栈。
    private final ArrayDeque<AbstractActor.Receive> behaviorStack = new ArrayDeque<>();
    // newActor 时暂存 ActorContext，使构造阶段的上下文语义与 Pekko 一致。
    private static final ThreadLocal<ArrayDeque<ActorContext>> contextStack = ThreadLocal.withInitial(ArrayDeque::new);
    private volatile Actor actor;
    // currentMessage 只在 invoke 期间有效，Actor 可以通过 sender() 读取它。
    private volatile Envelope currentMessage;
    private volatile boolean terminated;
    private volatile Envelope failedMessage;

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

    /**
     * 将已预留的 child name 替换为新 ActorRef。
     *
     * @param name  parent 下的 child name
     * @param child 已创建的 child 引用
     * @throws IllegalStateException 当 name 尚未预留时
     */
    public void childCreated(String name, ActorRef child) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (current.getByName(name) != ChildNameReserved.INSTANCE) {
                throw new IllegalStateException("child name was not reserved: " + name);
            }
            if (swapChildrenRefs(current, current.add(name, new ChildRestartStats(child)))) {
                publishLifecycle("now supervising " + child);
                return;
            }
        }
    }

    private void childTerminated(ActorRef child) {
        for (; ; ) {
            ChildrenContainer current = childrenRefs();
            if (swapChildrenRefs(current, current.remove(child))) return;
        }
    }

    private void watchedActorTerminated(SystemMessage.DeathWatchNotification notification) {
        if (childrenRefs().getByRef(notification.actor) != null) childTerminated(notification.actor);
        sendMessage(new Terminated(notification.actor), notification.actor);
    }

    private static Set<ActorRef> addWatcher(Set<ActorRef> watchers, ActorRef watcher) {
        if (watchers.contains(watcher)) return watchers;
        java.util.HashSet<ActorRef> updated = new java.util.HashSet<>(watchers);
        updated.add(watcher);
        return Set.copyOf(updated);
    }

    private static Set<ActorRef> removeWatcher(Set<ActorRef> watchers, ActorRef watcher) {
        if (!watchers.contains(watcher)) return watchers;
        java.util.HashSet<ActorRef> updated = new java.util.HashSet<>(watchers);
        updated.remove(watcher);
        return Set.copyOf(updated);
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
     * @param sendSupervise 保留 Pekko init 签名；当前本地实现尚未使用此参数
     */
    public void init(boolean sendSupervise) {
        dispatcher.systemDispatch(this, new SystemMessage.Create(null));
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
        if (message instanceof SystemMessage.Create create) create(create.failure);
        else if (message instanceof SystemMessage.Recreate recreate) recreate(recreate.cause);
        else if (message instanceof SystemMessage.Terminate) finishTerminate();
        else if (message instanceof SystemMessage.Suspend) mailbox.suspend();
        else if (message instanceof SystemMessage.Resume) mailbox.resume();
        else if (message instanceof SystemMessage.Watch watch) {
            if (!terminated) {
                int previousSize = watchedBy.size();
                watchedBy = addWatcher(watchedBy, watch.watcher);
                if (watchedBy.size() != previousSize) publishLifecycle("now watched by " + watch.watcher);
            }
            else if (watch.watcher instanceof LocalActorRef localWatcher) {
                localWatcher.underlying().sendSystemMessage(
                        new SystemMessage.DeathWatchNotification(watch.watchee, true, false));
            }
        } else if (message instanceof SystemMessage.Unwatch unwatch) {
            int previousSize = watchedBy.size();
            watchedBy = removeWatcher(watchedBy, unwatch.watcher);
            if (watchedBy.size() != previousSize) publishLifecycle("no longer watched by " + unwatch.watcher);
        }
        else if (message instanceof SystemMessage.Failed failed) handleFailure(failed);
        else if (message instanceof SystemMessage.DeathWatchNotification notification)
            watchedActorTerminated(notification);
    }

    /**
     * 处理 Create 消息并运行 Actor 构造及 preStart 生命周期。
     */
    private void create(Throwable failure) {
        if (failure != null) {
            terminated = true;
            return;
        }
        try {
            newActor().preStart();
            publishLifecycle("started (" + actor + ")");
        } catch (Throwable error) {
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
        ArrayDeque<ActorContext> stack = contextStack.get();
        stack.push(this);
        try {
            behaviorStack.clear();
            Actor instance = initialProps.newActor();
            if (instance == null)
                throw new ActorInitializationException("Actor instance passed to actorOf can't be null");
            if (instance instanceof AbstractActor abstractActor) abstractActor.attach(this);
            actor = instance;
            behaviorStack.push(instance.receive());
            return instance;
        } finally {
            stack.pop();
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
            handleInvokeFailure(error);
        } finally {
            currentMessage = null;
        }
    }

    /**
     * 根据 SupervisorStrategy 将失败转换为 Resume、Recreate 或 Terminate。
     */
    /**
     * 暂停当前 Mailbox 并把失败通知发送给 parent supervisor。
     */
    private void handleInvokeFailure(Throwable error) {
        failedMessage = currentMessage;
        mailbox.suspend();
        if (parent instanceof LocalActorRef localParent) {
            localParent.underlying().sendSystemMessage(new SystemMessage.Failed(self, error, self.path().uid()));
        } else {
            dispatcher.systemDispatch(this, new SystemMessage.Terminate());
        }
    }

    /**
     * 执行 Pekko FaultHandling 中的 preRestart、newActor、postRestart 顺序。
     */
    /**
     * 执行旧实例的 preRestart，并创建和启动新的 Actor 实例。
     */
    private void recreate(Throwable cause) {
        publishLifecycle("restarting");
        try {
            actor.preRestart(cause, failedMessage == null ? null : failedMessage.message());
        } catch (Throwable error) {
            publishError(error, error.getMessage());
        }
        try {
            newActor().postRestart(cause);
            failedMessage = null;
            mailbox.resume();
            publishLifecycle("restarted");
        } catch (Throwable error) {
            handleInvokeFailure(error);
        }
    }

    /**
     * 校验失败消息中的 child 引用和 UID，再应用当前 Actor 的监督策略。
     */
    private void handleFailure(SystemMessage.Failed failed) {
        ChildRestartStats child = childrenRefs().getByRef(failed.child);
        if (child == null || failed.uid != failed.child.path().uid()) return;
        if (!(failed.child instanceof LocalActorRef localChild)) return;
        ActorCell childCell = localChild.underlying();
        switch (actor.supervisorStrategy().handle(failed.cause)) {
            case Resume -> {
                publishFailure(failed, LogEvent.Level.WARNING);
                childCell.sendSystemMessage(new SystemMessage.Resume(failed.cause));
            }
            case Restart -> {
                publishFailure(failed, LogEvent.Level.ERROR);
                childCell.sendSystemMessage(new SystemMessage.Recreate(failed.cause));
            }
            case Stop -> {
                publishFailure(failed, LogEvent.Level.ERROR);
                childCell.sendSystemMessage(new SystemMessage.Terminate());
            }
            case Escalate -> handleInvokeFailure(failed.cause);
        }
    }

    private void publishFailure(SystemMessage.Failed failed, LogEvent.Level level) {
        String message = String.valueOf(failed.cause.getMessage());
        if (level == LogEvent.Level.WARNING) {
            system.eventStream().publish(new Warning(failed.child.path().toString(), SupervisorStrategy.class,
                    message, failed.cause));
        } else {
            system.eventStream().publish(new Error(failed.child.path().toString(), SupervisorStrategy.class,
                    message, failed.cause));
        }
    }


    /**
     * 完成终止流程：停止 children、调用 postStop、关闭 Mailbox 并通知观察者。
     */
    private void finishTerminate() {
        if (terminated) return;
        if (!childrenRefs().children().isEmpty()) publishLifecycle("stopping");
        ChildrenContainer current = childrenRefs();
        for (; ; ) {
            ChildrenContainer terminating = current.terminating();
            if (swapChildrenRefs(current, terminating)) break;
            current = childrenRefs();
        }
        for (ActorRef child : childrenRefs().children()) {
            child.stop();
            if (child instanceof LocalActorRef localChild) localChild.underlying().mailbox().processAllSystemMessages();
        }
        try {
            if (actor != null) actor.postStop();
        } catch (Exception error) {
            publishError(error, error.getMessage());
        }
        terminated = true;
        mailbox.close();
        mailbox.cleanUp();
        if (parent instanceof LocalActorRef localParent) {
            localParent.underlying().sendSystemMessage(
                    new SystemMessage.DeathWatchNotification(self, true, false));
        }
        for (ActorRef watcher : watchedBy) {
            if (watcher instanceof LocalActorRef localWatcher) {
                localWatcher.underlying().sendSystemMessage(new SystemMessage.DeathWatchNotification(self, true, false));
            }
        }
        watchedBy = Set.of();
        publishLifecycle("stopped");
        if (parent == null) system.actorTerminated(self);
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
        ((LocalActorRef) actor).underlying().sendSystemMessage(new SystemMessage.Watch(actor, self));
        return actor;
    }

    /**
     * 取消对此 Actor 的 DeathWatch 观察。
     */
    @Override
    public ActorRef unwatch(ActorRef actor) {
        ((LocalActorRef) actor).underlying().sendSystemMessage(new SystemMessage.Unwatch(actor, self));
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
