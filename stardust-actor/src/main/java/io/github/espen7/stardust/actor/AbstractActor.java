package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.event.Logging;
import io.github.espen7.stardust.event.LoggingAdapter;
import io.github.espen7.stardust.japi.pf.ReceiveBuilder;

import java.util.function.BiConsumer;

/**
 * Classic Actor 的 Java 基类，对应 Pekko AbstractActor。
 *
 * <p>用户实现 createReceive() 返回 Receive。ActorCell 在处理消息时调用当前
 * Receive，Actor 可以通过 getSelf、getSender 和 getContext 访问运行时上下文。</p>
 */
public abstract class AbstractActor implements Actor {
    /**
     * Receive 是已构建的消息匹配逻辑，由 ReceiveBuilder 创建。
     */
    public static final class Receive {
        private final BiConsumer<Object, ActorCell> handler;

        /**
         * 创建一个 Receive。
         *
         * @param handler 接收消息及其 ActorCell 的处理逻辑
         */
        public Receive(BiConsumer<Object, ActorCell> handler) {
            this.handler = handler;
        }

        /**
         * 由 ActorCell 调用，cell 参数让未匹配消息可以回到 Actor#unhandled。
         */
        void apply(Object message, ActorCell cell) {
            handler.accept(message, cell);
        }
    }

    private ActorCell context;
    private LoggingAdapter log;

    // Actor 实例由 ActorCell 创建后立即绑定上下文，用户代码不应自行调用 attach。
    final void attach(ActorCell context) {
        this.context = context;
    }

    /**
     * 返回当前 Actor 的上下文。
     *
     * <p>此对象只应在 Actor 的消息处理线程中使用。</p>
     *
     * @return 当前 ActorContext
     */
    public ActorContext getContext() {
        return context;
    }

    /**
     * @return 当前 Actor 的 ActorRef
     */
    public ActorRef getSelf() {
        return context.self();
    }

    /**
     * 返回当前正在处理的消息的 sender。
     *
     * <p>该值只在当前消息处理期间有意义。</p>
     *
     * @return 当前消息的 sender；没有 sender 时返回 dead letters 引用
     */
    public ActorRef getSender() {
        return context.sender();
    }

    /**
     * 返回以当前 Actor path 和 Actor class 作为 source 的日志适配器。
     *
     * <p>应在 Actor callback 中调用。日志会先发布到所属 ActorSystem 的 EventStream，再由
     * SLF4J bridge 转发给应用选择的日志 provider。</p>
     *
     * @return 当前 Actor 的日志适配器
     */
    public LoggingAdapter log() {
        if (context == null) throw new IllegalStateException("Actor logging is only available after actor creation");
        if (log == null) log = Logging.getLogger(context.system().eventStream(), getSelf().path().toString(), getClass());
        return log;
    }

    /**
     * 返回 child failure 的监督策略。
     *
     * <p>默认策略会重启抛出普通异常的 child，并停止初始化失败的 child。</p>
     *
     * @return 此 Actor 作为 supervisor 时使用的策略
     */
    @Override
    public SupervisorStrategy supervisorStrategy() {
        return SupervisorStrategy.defaultStrategy;
    }

    /**
     * @return 一个新的空 ReceiveBuilder
     */
    public ReceiveBuilder receiveBuilder() {
        return ReceiveBuilder.create();
    }

    /**
     * 定义 Actor 的初始消息处理逻辑。
     *
     * @return 该 Actor 创建时使用的 Receive
     */
    public AbstractActor.Receive createReceive() {
        return receiveBuilder().build();
    }

    /**
     * 返回 {@link #createReceive()} 定义的初始 behavior。
     */
    @Override
    public Receive receive() {
        return createReceive();
    }

    /**
     * 生命周期回调，ActorCell 创建实例后调用。
     */
    @Override
    public void preStart() throws Exception {
    }

    /**
     * 生命周期回调，ActorCell 完成终止前调用。
     */
    @Override
    public void postStop() throws Exception {
    }


    /**
     * 旧 Actor 实例重新创建前调用。
     *
     * @param reason  触发 restart 的异常
     * @param message 触发失败的消息；初始化失败时为 {@code null}
     * @throws Exception 子类生命周期钩子抛出的异常
     */
    @Override
    public void preRestart(Throwable reason, Object message) throws Exception {
        postStop();
    }

    /**
     * 新 Actor 实例创建后调用，默认执行 {@link #preStart()}。
     *
     * @param reason 触发 restart 的异常
     * @throws Exception 子类生命周期钩子抛出的异常
     */
    @Override
    public void postRestart(Throwable reason) throws Exception {
        preStart();
    }

    /**
     * 处理当前 Receive 未匹配的消息。
     *
     * <p>当前实现不发布 UnhandledMessage event。</p>
     */
    @Override
    public void unhandled(Object message) {
        if (context != null) {
            context.system().eventStream().publish(new UnhandledMessage(message, getSender(), getSelf()));
        }
    }
}
