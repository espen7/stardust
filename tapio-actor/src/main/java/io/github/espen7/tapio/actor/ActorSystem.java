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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.setup.ActorSystemSetup;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;

/**
 * ActorSystem 是一个独立 Actor runtime 的顶层入口和资源所有者。
 *
 * <p>每个 ActorSystem 拥有自己的 actor hierarchy、Dispatcher、Scheduler 和
 * ActorRefProvider。顶层 Actor 挂在 user guardian 下，子 Actor 则由其 parent 的
 * {@link ActorContext} 创建。ActorSystem 关闭时会停止 guardian，并在运行时资源
 * 完成关闭后完成 {@link #getWhenTerminated()} 返回的 stage。</p>
 *
 * <p>创建系统后，应使用 {@link #terminate()} 或 {@link #close()} 释放它持有的线程资源。
 * ActorSystem 本身不提供同步等待接口，调用者可以等待返回的
 * {@link CompletionStage}。</p>
 */
public abstract class ActorSystem implements ActorRefFactory, AutoCloseable {
    private static final String INTERNAL_DISPATCHER_ID = ActorPath.Scheme + ".actor.internal-dispatcher";
    /** 创建 ActorSystem 抽象实例。 */
    protected ActorSystem() {
    }

    /**
     * 创建一个使用默认名称 {@code default} 的本地 ActorSystem。
     *
     * @return 新建的 ActorSystem。调用者负责在使用完成后关闭它
     */
    public static ActorSystem create() {
        return create("default");
    }

    /**
     * 创建一个指定 system name 的本地 ActorSystem。
     *
     * <p>名称用于组成该系统的 ActorPath，例如
     * {@code tapio://<name>/user/<actor-name>}。</p>
     *
     * @param name system name
     * @return 新建的 ActorSystem
     * @throws IllegalArgumentException 当名称无效时
     */
    public static ActorSystem create(String name) {
        return new ActorSystemImpl(name);
    }

    /**
     * 创建使用指定配置的本地 ActorSystem。缺少的配置从 {@code reference.conf} 补齐。
     *
     * @param name 用于 ActorPath 的系统名称
     * @param config 应用配置。为 {@code null} 时按 {@link #create(String)} 加载配置
     * @return 新建的 ActorSystem
     * @throws IllegalArgumentException 当系统名称无效时
     * @throws ConfigurationException 当关闭配置相互冲突时
     * @throws com.typesafe.config.ConfigException 当配置缺失、类型错误或无法解析时
     */
    public static ActorSystem create(String name, Config config) {
        return new ActorSystemImpl(name, config);
    }

    /**
     * 使用编程式 {@link ActorSystemSetup} 创建本地 ActorSystem。
     *
     * @param name 用于 ActorPath 的系统名称
     * @param setups 编程式设置容器
     * @return 新建的 ActorSystem
     */
    public static ActorSystem create(String name, ActorSystemSetup setups) {
        return new ActorSystemImpl(name, null, setups);
    }

    /**
     * 使用指定配置与编程式 {@link ActorSystemSetup} 创建本地 ActorSystem。
     *
     * @param name 用于 ActorPath 的系统名称
     * @param config 应用配置（可为 {@code null}）
     * @param setups 编程式设置容器（可为 {@code null}）
     * @return 新建的 ActorSystem
     */
    public static ActorSystem create(String name, Config config, ActorSystemSetup setups) {
        return new ActorSystemImpl(name, config, setups);
    }

    /**
     * 保存 ActorSystem 的有效配置，以及本地 runtime 使用的已解析设置。
     *
     * <p>每个 ActorSystem 在启动时创建一份不可变的 Settings。</p>
     */
    public static final class Settings {
        private final String name;
        private final Config config;
        private final ClassLoader classLoader;
        /** 编程式传入的 {@link ActorSystemSetup}。 */
        public final ActorSystemSetup setup;

        /** 系统日志级别，默认 {@code INFO}。 */
        public final String LogLevel;
        /** 日志过滤器全限定类名。 */
        public final String LoggingFilter;
        /** 启动时是否打印生效配置，默认关闭。 */
        public final boolean LogConfigOnStart;
        /** ActorSystem 启动前的标准输出日志级别，默认 {@code WARNING}。 */
        public final String StdoutLogLevel;
        /** 启动时创建的默认 logger 类名；空列表使用 DefaultLogger。 */
        public final List<String> Loggers;
        /** 默认 logger Actor 使用的 Dispatcher id。 */
        public final String LoggersDispatcher;
        /** 等待默认 logger 初始化响应的时限，默认 5 秒。 */
        public final Duration LoggerStartTimeout;
        /** 每轮记录的 dead letters 上限。{@code 0} 表示关闭，{@link Integer#MAX_VALUE} 表示不限。 */
        public final int LogDeadLetters;
        /** 系统终止期间是否继续记录 dead letters，默认关闭。 */
        public final boolean LogDeadLettersDuringShutdown;
        /** 达到日志上限后的暂停时间。{@code null} 对应 Pekko 的无限期暂停。 */
        public final Duration LogDeadLettersSuspendDuration;
        /** 是否记录 Actor 生命周期的 DEBUG 日志，默认关闭。 */
        public final boolean DebugLifecycle;
        /** 是否记录自动接收消息的 DEBUG 日志，默认关闭。 */
        public final boolean DebugAutoReceive;
        /** 是否将未处理消息转为 DEBUG 日志，默认关闭。 */
        public final boolean DebugUnhandledMessage;
        /** 是否发布 EventStream 订阅和退订操作的 DEBUG 日志，默认关闭。 */
        public final boolean DebugEventStream;
        /** user guardian 的 SupervisorStrategyConfigurator 全限定类名。 */
        public final String SupervisorStrategyClass;
        /** 外部挂载的 Actor 切换 cell 时等待临时队列锁的时限，默认 10 秒。 */
        public final Duration UnstartedPushTimeout;
        /** Scheduler 实现的全限定类名。 */
        public final String SchedulerClass;
        /** runtime 创建的线程是否为 daemon，默认关闭。 */
        public final boolean Daemonicity;
        /** 未捕获 fatal error 是否通过 {@code System.exit(-1)} 关闭 JVM，默认开启。 */
        public final boolean JvmExitOnFatalError;
        /** CoordinatedShutdown 是否终止 ActorSystem，默认开启。 */
        public final boolean CoordinatedShutdownTerminateActorSystem;
        /** 调用 {@link ActorSystem#terminate()} 时是否运行 CoordinatedShutdown，默认开启。 */
        public final boolean CoordinatedShutdownRunByActorSystemTerminate;

        /** DeadLetterListener 日志中沿用的配置文本。 */
        final String logDeadLettersSuspendDurationDescription;

        Settings(ClassLoader classLoader, Config cfg, String name, ActorSystemSetup setup) {
            cfg.checkValid(ConfigFactory.defaultReference(classLoader)
                    .withoutPath(INTERNAL_DISPATCHER_ID), ActorPath.Scheme);
            this.config = cfg;
            this.classLoader = classLoader;
            this.name = name;
            this.setup = setup == null ? ActorSystemSetup.empty() : setup;
            LogLevel = config.getString(ActorPath.Scheme + ".loglevel");
            LoggingFilter = config.getString(ActorPath.Scheme + ".logging-filter");
            LogConfigOnStart = config.getBoolean(ActorPath.Scheme + ".log-config-on-start");
            StdoutLogLevel = config.getString(ActorPath.Scheme + ".stdout-loglevel");
            Loggers = List.copyOf(config.getStringList(ActorPath.Scheme + ".loggers"));
            LoggersDispatcher = config.getString(ActorPath.Scheme + ".loggers-dispatcher");
            LoggerStartTimeout = Duration.ofMillis(config.getDuration(ActorPath.Scheme + ".logger-startup-timeout").toMillis());
            LogDeadLetters = switch (config.getString(ActorPath.Scheme + ".log-dead-letters").toLowerCase(Locale.ROOT)) {
                case "off", "false" -> 0;
                case "on", "true" -> Integer.MAX_VALUE;
                default -> config.getInt(ActorPath.Scheme + ".log-dead-letters");
            };
            LogDeadLettersDuringShutdown = config.getBoolean(ActorPath.Scheme + ".log-dead-letters-during-shutdown");
            String key = ActorPath.Scheme + ".log-dead-letters-suspend-duration";
            logDeadLettersSuspendDurationDescription = config.getString(key);
            LogDeadLettersSuspendDuration = logDeadLettersSuspendDurationDescription.equalsIgnoreCase("infinite")
                    ? null : Duration.ofMillis(config.getDuration(key).toMillis());
            DebugLifecycle = config.getBoolean(ActorPath.Scheme + ".actor.debug.lifecycle");
            DebugAutoReceive = config.getBoolean(ActorPath.Scheme + ".actor.debug.autoreceive");
            DebugUnhandledMessage = config.getBoolean(ActorPath.Scheme + ".actor.debug.unhandled");
            DebugEventStream = config.getBoolean(ActorPath.Scheme + ".actor.debug.event-stream");
            SupervisorStrategyClass = config.getString(
                    ActorPath.Scheme + ".actor.guardian-supervisor-strategy");
            UnstartedPushTimeout = Duration.ofMillis(config.getDuration(
                    ActorPath.Scheme + ".actor.unstarted-push-timeout").toMillis());
            SchedulerClass = config.getString(ActorPath.Scheme + ".scheduler.implementation");
            Daemonicity = config.getBoolean(ActorPath.Scheme + ".daemonic");
            JvmExitOnFatalError = config.getBoolean(ActorPath.Scheme + ".jvm-exit-on-fatal-error");
            CoordinatedShutdownTerminateActorSystem =
                    config.getBoolean(ActorPath.Scheme + ".coordinated-shutdown.terminate-actor-system");
            CoordinatedShutdownRunByActorSystemTerminate =
                    config.getBoolean(ActorPath.Scheme + ".coordinated-shutdown.run-by-actor-system-terminate");
            if (CoordinatedShutdownRunByActorSystemTerminate && !CoordinatedShutdownTerminateActorSystem) {
                throw new ConfigurationException(
                        "tapio.coordinated-shutdown.run-by-actor-system-terminate=on and "
                                + "tapio.coordinated-shutdown.terminate-actor-system=off is not a supported configuration combination.");
            }
        }

        /**
         * 返回系统名称。
         *
         * @return 此 Settings 对应的 ActorSystem 名称
         */
        public String name() {
            return name;
        }

        /**
         * 返回已合并并解析的系统配置。
         *
         * @return 不可变的 Typesafe Config
         */
        public Config config() {
            return config;
        }

        /**
         * INTERNAL API：返回 ActorSystem 创建时使用的上下文类加载器。
         *
         * @return 类加载器
         */
        public ClassLoader classLoader() {
            return classLoader;
        }
    }

    /**
     * 返回该系统的 dead letters 引用。
     *
     * <p>向 dead letters 发送的消息不会交给 Actor 处理。当前实现会将它们作为
     * {@link DeadLetter} 事件发布到系统的 EventStream。</p>
     *
     * @return 此 ActorSystem 的 dead letters 引用
     */
    public abstract ActorRef deadLetters();

    /**
     * 返回此系统共享的 Dispatcher。
     *
     * <p>Actor 的 Mailbox 由 Dispatcher 调度。应用代码通常不需要直接操作它。</p>
     *
     * @return 此系统的 Dispatcher
     */
    @Override
    public abstract MessageDispatcher dispatcher();

    /**
     * 返回此系统的 Dispatcher 注册表。注册表与系统同生命周期。
     *
     * @return 此系统的 Dispatchers
     */
    public abstract Dispatchers dispatchers();

    /**
     * 返回此系统共享的 mailbox 注册表。
     *
     * @return 负责 mailbox type 选择和共享 dead-letter mailbox 的注册表
     */
    public abstract Mailboxes mailboxes();

    /**
     * 返回此系统的运行时设置。
     *
     * @return 系统启动时创建的 Settings
     */
    public abstract Settings settings();

    /**
     * 返回此系统共享的 Scheduler。
     *
     * <p>Scheduler 用于延迟执行 Runnable 或延迟向 ActorRef 投递消息。关闭
     * ActorSystem 时 Scheduler 会停止接收新任务。</p>
     *
     * @return 此系统的 Scheduler
     */
    public abstract Scheduler scheduler();

    /**
     * 返回此系统的事件总线。
     *
     * <p>日志以结构化 {@link io.github.espen7.tapio.event.LogEvent} 发布到此处；
     * 应用也可以订阅事件观察 Actor runtime 的行为。</p>
     *
     * @return 此 ActorSystem 独有的 EventStream
     */
    public abstract EventStream eventStream();

    /**
     * 返回带有此 ActorSystem source 信息的日志门面。
     *
     * @return 可发布到 EventStream 的日志适配器
     */
    public abstract LoggingAdapter log();

    /** 将当前系统的有效配置输出到 INFO 日志。 */
    public abstract void logConfiguration();

    /**
     * 异步停止 ActorSystem。
     *
     * <p>该调用发起系统关闭，并立即返回完成通知。user guardian 先终止，
     * 随后是 system guardian 和 root guardian。重复调用会返回同一个 stage。</p>
     *
     * @return ActorSystem 终止完成时的 stage，其结果描述位于 {@code /} 的 root guardian
     */
    public abstract CompletionStage<Terminated> terminate();

    /**
     * 返回 ActorSystem 的终止完成通知。
     *
     * <p>系统运行期间该 stage 保持未完成，调用 {@link #terminate()} 后，在关闭流程
     * 完成时正常完成。</p>
     *
     * @return ActorSystem 终止完成时的 stage
     */
    public abstract CompletionStage<Terminated> getWhenTerminated();

    /**
     * 注册在 {@link ActorSystem} 终止完成时执行的回调。若系统已终止，回调会立即执行。
     *
     * @param code 终止回调
     */
    public abstract void registerOnTermination(Runnable code);

    /**
     * 发起 ActorSystem 关闭。
     *
     * <p>此方法满足 {@link AutoCloseable}，并等待 Pekko 配置的
     * {@code close-actor-system-timeout}。超时时抛出 {@link java.util.concurrent.TimeoutException}。</p>
     *
     * @throws java.util.concurrent.TimeoutException 当关闭超时时
     */
    @Override
    public void close() throws Exception {
        terminate();
    }

    /**
     * 注册并创建指定 {@link ExtensionId} 对应的扩展实例；若已注册则直接返回已有实例。
     *
     * <p>该方法具有 {@code putIfAbsent} 语义；当另一线程正在初始化同一扩展时，当前线程会阻塞等待其完成。</p>
     *
     * @param ext 扩展标识
     * @param <T> 扩展实例类型
     * @return 扩展实例
     */
    public abstract <T extends Extension> T registerExtension(ExtensionId<T> ext);

    /**
     * 返回指定 {@link ExtensionId} 关联的扩展实例；若尚未注册则抛出异常。
     *
     * <p>当另一线程正在初始化同一扩展时，当前线程会阻塞等待其完成。</p>
     *
     * @param ext 扩展标识
     * @param <T> 扩展实例类型
     * @return 已注册的扩展实例
     * @throws IllegalArgumentException 当扩展未注册时
     */
    public abstract <T extends Extension> T extension(ExtensionId<T> ext);

    /**
     * 返回指定 {@link ExtensionId} 是否已在此系统中注册。
     *
     * <p>当另一线程正在初始化同一扩展时，当前线程会阻塞等待其完成。</p>
     *
     * @param ext 扩展标识
     * @return 已注册返回 {@code true}
     */
    public abstract boolean hasExtension(ExtensionId<? extends Extension> ext);
}
