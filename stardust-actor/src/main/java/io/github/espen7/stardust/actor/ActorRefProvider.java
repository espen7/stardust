package io.github.espen7.stardust.actor;

/**
 * ActorRef 和 ActorPath 的创建边界，对应 Pekko ActorRefProvider。
 */
public abstract class ActorRefProvider {

    public abstract ActorRef actorOf(Props props, String name, ActorRef parent);

    public abstract ActorRef rootGuardian();

    /** 返回 system guardian。 */
    public abstract ActorRef systemGuardian();

    /** 创建挂在 system guardian 下的系统 Actor。 */
    public abstract ActorRef systemActorOf(Props props, String name);

    public abstract ActorRef deadLetters();
    /**
     * 返回调度器。
     *
     * @return 所属 ActorSystem 的共享 {@link Scheduler}
     */
    public abstract Scheduler scheduler();
    /**
     * 分配无自定义前缀的临时路径。
     *
     * @return 尚未注册的临时路径
     */
    public ActorPath tempPath() {
        return tempPath("");
    }

    /**
     * 分配在当前系统内唯一的临时路径。
     *
     * @param prefix 临时路径最后一段的前缀，可以为空字符串
     * @return 尚未注册的临时路径
     */
    public abstract ActorPath tempPath(String prefix);
    /**
     * 返回临时引用的逻辑父级。
     *
     * @return 存放已注册临时引用的容器
     */
    public abstract InternalActorRef tempContainer();

    /**
     * 注册内部引用，使其可以在临时容器中按路径名称查找。
     *
     * @param actorRef 要注册的内部引用
     * @param path 由此 Provider 的 {@link #tempPath(String)} 分配的路径
     */
    public abstract void registerTempActor(InternalActorRef actorRef, ActorPath path);

    /**
     * 注销临时容器中的引用。
     *
     * @param path 此 Provider 此前注册的临时路径
     */
    public abstract void unregisterTempActor(ActorPath path);
}
