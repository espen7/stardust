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

import io.github.espen7.tapio.annotation.InternalApi;

/**
 * INTERNAL API：Tapio 运行时内部使用的配置路径常量表。
 *
 * <p>集中维护以 {@code tapio.} 为根的配置键，避免在各组件中重复拼接。此常量表属于
 * Java 移植层的内部收口实现，不构成 Pekko 对外公开契约。</p>
 */
@InternalApi
public final class ConfigKeys {

    private ConfigKeys() {
    }

    /**
     * 默认日志级别配置键。
     */
    public static final String LOG_LEVEL = "tapio.loglevel";
    /**
     * 日志过滤器的全限定类名配置键。
     */
    public static final String LOGGING_FILTER = "tapio.logging-filter";
    /**
     * 启动时是否打印完整配置的配置键。
     */
    public static final String LOG_CONFIG_ON_START = "tapio.log-config-on-start";
    /**
     * 标准输出日志级别配置键。
     */
    public static final String STDOUT_LOG_LEVEL = "tapio.stdout-loglevel";
    /**
     * 启动的 Logger 列表配置键。
     */
    public static final String LOGGERS = "tapio.loggers";
    /**
     * Logger 使用的 Dispatcher 配置键。
     */
    public static final String LOGGERS_DISPATCHER = "tapio.loggers-dispatcher";
    /**
     * Logger 启动超时配置键。
     */
    public static final String LOGGER_STARTUP_TIMEOUT = "tapio.logger-startup-timeout";
    /**
     * 死信日志数量或开关配置键。
     */
    public static final String LOG_DEAD_LETTERS = "tapio.log-dead-letters";
    /**
     * 关闭期间是否记录死信配置键。
     */
    public static final String LOG_DEAD_LETTERS_DURING_SHUTDOWN = "tapio.log-dead-letters-during-shutdown";
    /**
     * 死信日志暂停时长配置键。
     */
    public static final String LOG_DEAD_LETTERS_SUSPEND_DURATION = "tapio.log-dead-letters-suspend-duration";

    /**
     * 是否开启 {@code LoggingReceive} 调试日志的配置键。
     */
    public static final String ACTOR_DEBUG_RECEIVE = "tapio.actor.debug.receive";
    /**
     * 是否开启自动处理消息调试日志的配置键。
     */
    public static final String ACTOR_DEBUG_AUTORECEIVE = "tapio.actor.debug.autoreceive";
    /**
     * 是否开启 Actor 生命周期调试日志的配置键。
     */
    public static final String ACTOR_DEBUG_LIFECYCLE = "tapio.actor.debug.lifecycle";
    /**
     * 是否开启 EventStream 调试日志的配置键。
     */
    public static final String ACTOR_DEBUG_EVENT_STREAM = "tapio.actor.debug.event-stream";

    /**
     * 是否开启未处理消息调试日志的配置键。
     */
    public static final String ACTOR_DEBUG_UNHANDLED = "tapio.actor.debug.unhandled";

    /**
     * user guardian 监督策略配置器类名配置键。
     */
    public static final String ACTOR_GUARDIAN_SUPERVISOR_STRATEGY = "tapio.actor.guardian-supervisor-strategy";

    /**
     * Actor 创建超时配置键。
     */
    public static final String ACTOR_CREATION_TIMEOUT = "tapio.actor.creation-timeout";

    /**
     * 外部挂载 Actor 切换 cell 的临时队列锁超时配置键。
     */
    public static final String ACTOR_UNSTARTED_PUSH_TIMEOUT = "tapio.actor.unstarted-push-timeout";

    /**
     * Actor 部署配置段路径。
     */
    public static final String ACTOR_DEPLOYMENT = "tapio.actor.deployment";

    /**
     * 默认 Dispatcher 的配置路径。
     */
    public static final String ACTOR_DEFAULT_DISPATCHER = "tapio.actor.default-dispatcher";

    /**
     * 内部 Dispatcher 的配置路径。
     */
    public static final String ACTOR_INTERNAL_DISPATCHER = "tapio.actor.internal-dispatcher";

    /**
     * 默认 Mailbox 的配置路径。
     */
    public static final String ACTOR_DEFAULT_MAILBOX = "tapio.actor.default-mailbox";

    /**
     * Mailbox 类型需求映射配置段路径。
     */
    public static final String ACTOR_MAILBOX_REQUIREMENTS = "tapio.actor.mailbox.requirements";

    /**
     * Scheduler 实现类名配置键。
     */
    public static final String SCHEDULER_IMPLEMENTATION = "tapio.scheduler.implementation";

    /**
     * 时间轮槽位数配置键。
     */
    public static final String SCHEDULER_TICKS_PER_WHEEL = "tapio.scheduler.ticks-per-wheel";

    /**
     * 时间轮 tick 间隔配置键。
     */
    public static final String SCHEDULER_TICK_DURATION = "tapio.scheduler.tick-duration";

    /**
     * tick 间隔校验失败时是否抛异常的配置键。
     */
    public static final String SCHEDULER_ERROR_ON_TICK_DURATION_VERIFICATION_FAILED =
            "tapio.scheduler.error-on-tick-duration-verification-failed";
    /**
     * Scheduler 关闭超时配置键。
     */
    public static final String SCHEDULER_SHUTDOWN_TIMEOUT = "tapio.scheduler.shutdown-timeout";

    /**
     * 线程是否为守护线程的配置键。
     */
    public static final String DAEMONIC = "tapio.daemonic";

    /**
     * 未捕获致命错误时是否退出 JVM 的配置键。
     */
    public static final String JVM_EXIT_ON_FATAL_ERROR = "tapio.jvm-exit-on-fatal-error";

    /**
     * 库级扩展列表配置键。
     */
    public static final String LIBRARY_EXTENSIONS = "tapio.library-extensions";

    /**
     * 应用级扩展列表配置键。
     */
    public static final String EXTENSIONS = "tapio.extensions";

    /**
     * CoordinatedShutdown 配置段路径。
     */
    public static final String COORDINATED_SHUTDOWN = "tapio.coordinated-shutdown";

    /**
     * CoordinatedShutdown 是否终止 ActorSystem 的配置键。
     */
    public static final String COORDINATED_SHUTDOWN_TERMINATE_ACTOR_SYSTEM = "tapio.coordinated-shutdown.terminate-actor-system";

    /**
     * 调用 {@code ActorSystem.terminate()} 时是否运行 CoordinatedShutdown 的配置键。
     */
    public static final String COORDINATED_SHUTDOWN_RUN_BY_ACTOR_SYSTEM_TERMINATE = "tapio.coordinated-shutdown.run-by-actor-system-terminate";

    /**
     * 是否由 JVM shutdown hook 触发 CoordinatedShutdown 的配置键。
     */
    public static final String COORDINATED_SHUTDOWN_RUN_BY_JVM_SHUTDOWN_HOOK = "tapio.coordinated-shutdown.run-by-jvm-shutdown-hook";

    /**
     * 等待 ActorSystem 终止的超时配置键。
     */
    public static final String COORDINATED_SHUTDOWN_CLOSE_ACTOR_SYSTEM_TIMEOUT = "tapio.coordinated-shutdown.close-actor-system-timeout";

    /**
     * CoordinatedShutdown 阶段配置段路径。
     */
    public static final String COORDINATED_SHUTDOWN_PHASES = "tapio.coordinated-shutdown.phases";

    /**
     * CoordinatedShutdown 默认阶段超时配置键。
     */
    public static final String COORDINATED_SHUTDOWN_DEFAULT_PHASE_TIMEOUT = "tapio.coordinated-shutdown.default-phase-timeout";
}
