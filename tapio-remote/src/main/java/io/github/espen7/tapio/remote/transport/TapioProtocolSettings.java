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
package io.github.espen7.tapio.remote.transport;

import com.typesafe.config.Config;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.remote.RemoteSettings;

import java.time.Duration;
import java.util.List;

/**
 * INTERNAL API：Tapio 协议传输适配器配置。
 */
@InternalApi
public final class TapioProtocolSettings {
    /** 传输层失效检测器配置节。 */
    public final Config TransportFailureDetectorConfig;
    /** 传输层失效检测器实现类名。 */
    public final String TransportFailureDetectorImplementationClass;
    /** 传输层心跳发送间隔。 */
    public final Duration TransportHeartBeatInterval;
    /** 握手超时。 */
    public final Duration HandshakeTimeout;
    /** 协议管理器名称前缀。 */
    public final String ManagerNamePrefix;
    /** 协议 scheme 名称。 */
    public final String TapioScheme;

    /**
     * 从系统配置构造协议传输设置。
     *
     * @param config 系统配置
     */
    public TapioProtocolSettings(Config config) {
        this.TransportFailureDetectorConfig = config.getConfig("tapio.remote.classic.transport-failure-detector");
        this.TransportFailureDetectorImplementationClass =
                TransportFailureDetectorConfig.getString("implementation-class");
        this.TransportHeartBeatInterval = TransportFailureDetectorConfig.getDuration("heartbeat-interval");
        List<String> enabled = config.getStringList("tapio.remote.classic.enabled-transports");
        if (enabled.contains("tapio.remote.classic.netty.tcp")) {
            this.HandshakeTimeout = config.getDuration("tapio.remote.classic.netty.tcp.connection-timeout");
        } else {
            this.HandshakeTimeout = config.getDuration("tapio.remote.classic.handshake-timeout");
        }
        this.ManagerNamePrefix = config.getString("tapio.remote.classic.manager-name-prefix");
        this.TapioScheme = new RemoteSettings(config).ProtocolName;
    }
}
