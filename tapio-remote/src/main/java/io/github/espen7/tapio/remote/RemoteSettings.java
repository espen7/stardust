/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.remote;

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.event.LogEvent;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Classic Remoting 配置视图。
 */
public final class RemoteSettings {
    private static final Set<String> ALLOWABLE_PROTOCOL_NAMES = Set.of(ActorPath.Scheme);

    /** 原始配置对象。 */
    public final Config config;
    /** 是否记录收到的远程消息。 */
    public final boolean LogReceive;
    /** 是否记录发出的远程消息。 */
    public final boolean LogSend;
    /** 帧大小告警阈值。 */
    public final Optional<Integer> LogFrameSizeExceeding;
    /** 是否启用非信任模式。 */
    public final boolean UntrustedMode;
    /** 非信任模式下允许的 ActorSelection 路径集合。 */
    public final Set<String> TrustedSelectionPaths;
    /** 远程生命周期事件日志级别。 */
    public final LogEvent.Level RemoteLifecycleEventsLogLevel;
    /** 远程内部 Actor 使用的 Dispatcher id。 */
    public final String Dispatcher;
    /** 关闭超时。 */
    public final Duration ShutdownTimeout;
    /** 关闭前刷新写缓冲等待时长。 */
    public final Duration FlushWait;
    /** 启动超时。 */
    public final Duration StartupTimeout;
    /** 重试门控关闭时长。 */
    public final Duration RetryGateClosedFor;
    /** 是否复用入站连接作为被动出站连接。 */
    public final boolean UsePassiveConnections;
    /** 被动连接切换缓冲上限。 */
    public final int PassiveConnectionBufferSize;
    /** 写拒绝后的退避间隔。 */
    public final Duration BackoffPeriod;
    /** 退避缓冲告警条数上限。 */
    public final int LogBufferSizeExceeding;
    /** 系统消息 ACK 捎带超时。 */
    public final Duration SysMsgAckTimeout;
    /** 系统消息重传间隔。 */
    public final Duration SysResendTimeout;
    /** 系统消息重传次数上限。 */
    public final int SysResendLimit;
    /** 系统消息缓冲上限。 */
    public final int SysMsgBufferSize;
    /** 初始系统消息投递超时。 */
    public final Duration InitialSysMsgDeliveryTimeout;
    /** 静默隔离超时。 */
    public final Duration QuarantineSilentSystemTimeout;
    /** 隔离标记清理时长。 */
    public final Duration QuarantineDuration;
    /** 管理命令确认超时。 */
    public final Duration CommandAckTimeout;
    /** 是否允许在无 Cluster 时使用不安全远程特性。 */
    public final boolean UseUnsafeRemoteFeaturesWithoutCluster;
    /** 无 Cluster 使用 watch 时是否输出警告。 */
    public final boolean WarnUnsafeWatchWithoutCluster;
    /** Watch 失效检测器配置节。 */
    public final Config WatchFailureDetectorConfig;
    /** Watch 失效检测器实现类。 */
    public final String WatchFailureDetectorImplementationClass;
    /** Watch 心跳发送间隔。 */
    public final Duration WatchHeartBeatInterval;
    /** Watch 不可达节点清理间隔。 */
    public final Duration WatchUnreachableReaperInterval;
    /** Watch 期望心跳响应超时。 */
    public final Duration WatchHeartbeatExpectedResponseAfter;
    /** 启用的传输配置列表。 */
    public final List<TransportConfig> Transports;
    /** 协议 scheme 名称。 */
    public final String ProtocolName;
    /** 可接受的协议 scheme 名称集合。 */
    public final Set<String> AcceptProtocolNames;

    /**
     * 单个传输驱动配置描述。
     *
     * @param transportClass 传输实现类名
     * @param adapters 适配器列表
     * @param config 传输配置节
     */
    public record TransportConfig(String transportClass, List<String> adapters, Config config) {
    }

    /**
     * 从系统配置构造 Classic Remoting 配置视图。
     *
     * @param config 系统配置
     */
    public RemoteSettings(Config config) {
        this.config = config;
        this.LogReceive = config.getBoolean("tapio.remote.classic.log-received-messages");
        this.LogSend = config.getBoolean("tapio.remote.classic.log-sent-messages");
        String frameLimit = config.getString("tapio.remote.classic.log-frame-size-exceeding");
        this.LogFrameSizeExceeding = "off".equalsIgnoreCase(frameLimit)
                ? Optional.empty()
                : Optional.of((int) config.getBytes("tapio.remote.classic.log-frame-size-exceeding").longValue());
        this.UntrustedMode = config.getBoolean("tapio.remote.classic.untrusted-mode");
        this.TrustedSelectionPaths = Set.copyOf(config.getStringList("tapio.remote.classic.trusted-selection-paths"));
        String lifecycleStr = config.getString("tapio.remote.classic.log-remote-lifecycle-events")
                .toLowerCase(Locale.ROOT);
        this.RemoteLifecycleEventsLogLevel = switch (lifecycleStr) {
            case "on", "debug" -> LogEvent.Level.DEBUG;
            case "off" -> LogEvent.Level.OFF;
            case "info" -> LogEvent.Level.INFO;
            case "warning" -> LogEvent.Level.WARNING;
            case "error" -> LogEvent.Level.ERROR;
            default -> throw new ConfigurationException(
                    "Logging level must be one of (on, off, debug, info, warning, error)");
        };
        this.Dispatcher = config.getString("tapio.remote.classic.use-dispatcher");
        this.ShutdownTimeout = requirePositive(config.getDuration("tapio.remote.classic.shutdown-timeout"),
                "shutdown-timeout must be > 0");
        this.FlushWait = requirePositive(config.getDuration("tapio.remote.classic.flush-wait-on-shutdown"),
                "flush-wait-on-shutdown must be > 0");
        this.StartupTimeout = requirePositive(config.getDuration("tapio.remote.classic.startup-timeout"),
                "startup-timeout must be > 0");
        this.RetryGateClosedFor = requireNonNegative(config.getDuration("tapio.remote.classic.retry-gate-closed-for"),
                "retry-gate-closed-for must be >= 0");
        this.UsePassiveConnections = config.getBoolean("tapio.remote.classic.use-passive-connections");
        this.PassiveConnectionBufferSize = config.getInt("tapio.remote.classic.passive-connection-buffer-size");
        this.BackoffPeriod = requirePositive(config.getDuration("tapio.remote.classic.backoff-interval"),
                "backoff-interval must be > 0");
        String bufKey = "tapio.remote.classic.log-buffer-size-exceeding";
        String bufStr = config.getString(bufKey).toLowerCase(Locale.ROOT);
        this.LogBufferSizeExceeding = ("off".equals(bufStr) || "false".equals(bufStr))
                ? Integer.MAX_VALUE
                : config.getInt(bufKey);
        this.SysMsgAckTimeout = requirePositive(
                config.getDuration("tapio.remote.classic.system-message-ack-piggyback-timeout"),
                "system-message-ack-piggyback-timeout must be > 0");
        this.SysResendTimeout = requirePositive(config.getDuration("tapio.remote.classic.resend-interval"),
                "resend-interval must be > 0");
        this.SysResendLimit = config.getInt("tapio.remote.classic.resend-limit");
        if (this.SysResendLimit <= 0) throw new IllegalArgumentException("resend-limit must be > 0");
        this.SysMsgBufferSize = config.getInt("tapio.remote.classic.system-message-buffer-size");
        if (this.SysMsgBufferSize <= 0) throw new IllegalArgumentException("system-message-buffer-size must be > 0");
        this.InitialSysMsgDeliveryTimeout = requirePositive(
                config.getDuration("tapio.remote.classic.initial-system-message-delivery-timeout"),
                "initial-system-message-delivery-timeout must be > 0");
        String qKey = "tapio.remote.classic.quarantine-after-silence";
        String qStr = config.getString(qKey).toLowerCase(Locale.ROOT);
        this.QuarantineSilentSystemTimeout = ("off".equals(qStr) || "false".equals(qStr))
                ? Duration.ZERO
                : requirePositive(config.getDuration(qKey), "quarantine-after-silence must be > 0");
        this.QuarantineDuration = requirePositive(
                config.getDuration("tapio.remote.classic.prune-quarantine-marker-after"),
                "prune-quarantine-marker-after must be > 0 ms");
        this.CommandAckTimeout = requirePositive(config.getDuration("tapio.remote.classic.command-ack-timeout"),
                "command-ack-timeout must be > 0");
        this.UseUnsafeRemoteFeaturesWithoutCluster =
                config.getBoolean("tapio.remote.use-unsafe-remote-features-outside-cluster");
        this.WarnUnsafeWatchWithoutCluster = config.getBoolean("tapio.remote.warn-unsafe-watch-outside-cluster");
        this.WatchFailureDetectorConfig = config.getConfig("tapio.remote.watch-failure-detector");
        this.WatchFailureDetectorImplementationClass = WatchFailureDetectorConfig.getString("implementation-class");
        this.WatchHeartBeatInterval = requirePositive(WatchFailureDetectorConfig.getDuration("heartbeat-interval"),
                "watch-failure-detector.heartbeat-interval must be > 0");
        this.WatchUnreachableReaperInterval = requirePositive(
                WatchFailureDetectorConfig.getDuration("unreachable-nodes-reaper-interval"),
                "watch-failure-detector.unreachable-nodes-reaper-interval must be > 0");
        this.WatchHeartbeatExpectedResponseAfter = requirePositive(
                WatchFailureDetectorConfig.getDuration("expected-response-after"),
                "watch-failure-detector.expected-response-after > 0");

        List<String> transportNames = config.getStringList("tapio.remote.classic.enabled-transports");
        this.Transports = transportNames.stream().map(name -> {
            Config tc = config.getConfig(name);
            return new TransportConfig(
                    tc.getString("transport-class"),
                    List.copyOf(tc.getStringList("applied-adapters").reversed()),
                    tc);
        }).toList();

        String proto = config.getString("tapio.remote.protocol-name");
        if (!ALLOWABLE_PROTOCOL_NAMES.contains(proto)) {
            throw new ConfigurationException(
                    "The only allowed values for tapio.remote.protocol-name are \"" + ActorPath.Scheme + "\".");
        }
        this.ProtocolName = proto;
        Set<String> accept = Set.copyOf(config.getStringList("tapio.remote.accept-protocol-names"));
        if (accept.isEmpty()) {
            throw new ConfigurationException("tapio.remote.accept-protocol-names setting must not be empty.");
        }
        for (String s : accept) {
            if (!ALLOWABLE_PROTOCOL_NAMES.contains(s)) {
                throw new ConfigurationException(
                        "tapio.remote.accept-protocol-names only accepts \"" + ActorPath.Scheme + "\".");
            }
        }
        this.AcceptProtocolNames = accept;
    }

    /**
     * 为远程内部 Actor 配置 Dispatcher。
     *
     * @param props 原始 Props
     * @return 配置后的 Props
     */
    public Props configureDispatcher(Props props) {
        return Dispatcher.isEmpty() ? props : props.withDispatcher(Dispatcher);
    }

    private static Duration requirePositive(Duration d, String msg) {
        if (d.compareTo(Duration.ZERO) <= 0) throw new IllegalArgumentException(msg);
        return d;
    }

    private static Duration requireNonNegative(Duration d, String msg) {
        if (d.compareTo(Duration.ZERO) < 0) throw new IllegalArgumentException(msg);
        return d;
    }
}
