package io.github.espen7.stardust.actor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.event.EventStream;
import io.github.espen7.stardust.event.LogEvent;
import io.github.espen7.stardust.event.Logging;
import io.github.espen7.stardust.event.LoggingAdapter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * ActorSystem 的本地实现。
 *
 * <p>初始化顺序贴近 Pekko 的 ActorSystemImpl：先准备 Dispatcher、Scheduler
 * 和 Provider，再由 Provider 创建 user guardian。所有顶层 Actor 都挂在 user
 * guardian 下面，所以关闭时从 guardian 开始停止整棵 Actor tree。</p>
 */
public class ActorSystemImpl extends ActorSystem {

    private final String name;
    private final Config config;
    private final Dispatcher dispatcher;
    private final Scheduler scheduler;
    private final EventStream eventStream;
    private final LoggingAdapter log;
    private final LocalActorRefProvider provider;
    private final CompletableFuture<Terminated> whenTerminated = new CompletableFuture<>();

    public ActorSystemImpl(String name) {
        this.name = name;
        this.config = ConfigFactory.load();
        this.dispatcher = new Dispatcher(config.getInt("pekko.actor.default-dispatcher.throughput"));
        this.scheduler = new Scheduler();
        this.eventStream = new EventStream(LogEvent.Level.parse(config.getString("pekko.actor.loglevel")));
        this.log = Logging.getLogger(eventStream, "pekko://" + name, getClass());
        this.provider = new LocalActorRefProvider(this);
        Logging.installSlf4jLogger(this, debugUnhandled());
    }

    public String name() {
        return name;
    }

    public LocalActorRefProvider provider() {
        return provider;
    }

    @Override
    public ActorRef actorOf(Props props, String name) {
        return ((LocalActorRef) provider.rootGuardian()).underlying().actorOf(props, name);
    }

    @Override
    public ActorRef deadLetters() {
        return provider.deadLetters();
    }

    @Override
    public Dispatcher dispatcher() {
        return dispatcher;
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public EventStream eventStream() {
        return eventStream;
    }

    @Override
    public LoggingAdapter log() {
        return log;
    }

    boolean debugLifecycle() {
        return config.getBoolean("pekko.actor.debug.lifecycle");
    }

    boolean debugUnhandled() {
        return config.getBoolean("pekko.actor.debug.unhandled");
    }

    /**
     * 发起系统关闭。
     *
     * <p>Terminate 是幂等的。Actor tree 先结束，Scheduler 和 Dispatcher 随后
     * 释放，最后完成 whenTerminated。这是阅读 Pekko 关闭流程时的重要边界。</p>
     */
    @Override
    public CompletionStage<Terminated> terminate() {
        provider.rootGuardian().stop();
        return whenTerminated;
    }

    @Override
    public CompletionStage<Terminated> getWhenTerminated() {
        return whenTerminated;
    }

    void actorTerminated(ActorRef actor) {
        scheduler.close();
        dispatcher.close();
        whenTerminated.complete(new Terminated(actor));
    }
}
