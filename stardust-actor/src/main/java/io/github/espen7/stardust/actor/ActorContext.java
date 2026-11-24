package io.github.espen7.stardust.actor;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Actor 执行期间可访问的上下文。
 *
 * <p>ActorContext 暴露子 Actor 创建、停止、watch 和 behavior 切换操作。它不是
 * thread-safe API，调用者应在 Actor message processing thread 中使用。</p>
 */
public interface ActorContext {
    /**
     * 返回所属 ActorSystem。
     */
    ActorSystem system();

    /**
     * 返回当前 Actor 的引用。
     */
    ActorRef self();

    /**
     * 返回当前 Envelope 中的 sender。
     */
    ActorRef sender();

    /**
     * 使用自动生成的 name 创建 child。
     */
    ActorRef actorOf(Props props);

    /**
     * 使用指定 name 创建 child。
     */
    ActorRef actorOf(Props props, String name);

    /**
     * 异步停止目标 Actor。
     */
    void stop(ActorRef actor);

    /**
     * 建立 DeathWatch 关系。
     */
    ActorRef watch(ActorRef actor);

    /**
     * 解除 DeathWatch 关系。
     */
    ActorRef unwatch(ActorRef actor);

    /**
     * 替换当前 behavior。
     */
    void become(AbstractActor.Receive behavior);

    /**
     * 根据 discardOld 决定替换或压入当前 behavior。
     */
    void become(AbstractActor.Receive behavior, boolean discardOld);

    /**
     * 弹出当前 behavior，恢复前一个 behavior。
     */
    void unbecome();

    /**
     * 返回当前 Actor 的 child 引用快照。
     */
    Collection<ActorRef> getChildren();

    /**
     * 返回 ActorSystem 终止完成时的 stage。
     */
    default CompletionStage<Terminated> getWhenTerminated() {
        return system().getWhenTerminated();
    }
}
