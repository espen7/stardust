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
package io.github.espen7.tapio.remote.transport.netty;

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Locale;

/**
 * Netty TCP 传输配置视图。
 */
public final class NettyTransportSettings {
    /** 协议 scheme 名称。 */
    public final String TransportMode = "tcp";
    /** 对外宣告主机名。 */
    public final String Hostname;
    /** 对外宣告端口（0 表示使用实际绑定端口）。 */
    public final int Port;
    /** 实际绑定主机名。 */
    public final String BindHostname;
    /** 实际绑定端口。 */
    public final int BindPort;
    /** 连接超时。 */
    public final Duration ConnectionTimeout;
    /** 最大帧字节数（至少 32000）。 */
    public final int MaxFrameSize;
    /** 服务端 accept backlog。 */
    public final int Backlog;
    /** TCP_NODELAY 开关。 */
    public final boolean TcpNodelay;
    /** SO_KEEPALIVE 开关。 */
    public final boolean TcpKeepalive;
    /** SO_REUSEADDR 开关。 */
    public final boolean TcpReuseAddr;
    /** 发送缓冲区大小（0 表示系统默认）。 */
    public final int SendBufferSize;
    /** 接收缓冲区大小（0 表示系统默认）。 */
    public final int ReceiveBufferSize;
    /** 写高水位线（0 表示系统默认）。 */
    public final int WriteBufferHighWaterMark;
    /** 写低水位线（0 表示系统默认）。 */
    public final int WriteBufferLowWaterMark;
    /** 服务端 worker 线程数。 */
    public final int ServerSocketWorkerPoolSize;
    /** 客户端 worker 线程数。 */
    public final int ClientSocketWorkerPoolSize;

    /**
     * 从 Netty 传输配置节加载设置。
     *
     * @param config 传输配置节
     */
    public NettyTransportSettings(Config config) {
        String rawHost = config.getString("hostname");
        this.Hostname = rawHost.isEmpty() ? defaultHostAddress() : rawHost;
        this.Port = config.getInt("port");
        if (this.Port < 0 || this.Port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535, got " + this.Port);
        }
        String rawBindHost = config.getString("bind-hostname");
        this.BindHostname = rawBindHost.isEmpty() ? this.Hostname : rawBindHost;
        String rawBindPort = config.getString("bind-port");
        this.BindPort = rawBindPort.isEmpty() ? this.Port : Integer.parseInt(rawBindPort);
        this.ConnectionTimeout = config.getDuration("connection-timeout");
        long maxFrame = config.getBytes("maximum-frame-size");
        if (maxFrame < 32000L) {
            throw new ConfigurationException("Setting 'maximum-frame-size' must be at least 32000 bytes");
        }
        this.MaxFrameSize = (int) Math.min(maxFrame, Integer.MAX_VALUE);
        this.Backlog = config.getInt("backlog");
        this.TcpNodelay = config.getBoolean("tcp-nodelay");
        this.TcpKeepalive = config.getBoolean("tcp-keepalive");
        String reuseStr = config.getString("tcp-reuse-addr").toLowerCase(Locale.ROOT);
        this.TcpReuseAddr = switch (reuseStr) {
            case "off-for-windows" -> !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
            case "on", "true" -> true;
            default -> false;
        };
        this.SendBufferSize = config.getBytes("send-buffer-size").intValue();
        this.ReceiveBufferSize = config.getBytes("receive-buffer-size").intValue();
        this.WriteBufferHighWaterMark = config.getBytes("write-buffer-high-water-mark").intValue();
        this.WriteBufferLowWaterMark = config.getBytes("write-buffer-low-water-mark").intValue();
        this.ServerSocketWorkerPoolSize = computePoolSize(config.getConfig("server-socket-worker-pool"));
        this.ClientSocketWorkerPoolSize = computePoolSize(config.getConfig("client-socket-worker-pool"));
    }

    private static int computePoolSize(Config poolCfg) {
        int min = poolCfg.getInt("pool-size-min");
        double factor = poolCfg.getDouble("pool-size-factor");
        int max = poolCfg.getInt("pool-size-max");
        int ceil = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * factor);
        return Math.max(min, Math.min(max, ceil));
    }

    private static String defaultHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
