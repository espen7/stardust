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
package io.github.espen7.tapio.cluster.singleton;

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorSystem;

import java.time.Duration;
import java.util.Optional;

/**
 * {@link ClusterSingletonProxy} 配置参数。
 */
public final class ClusterSingletonProxySettings {
    private final String singletonName;
    private final Optional<String> role;
    private final Duration singletonIdentificationInterval;
    private final int bufferSize;

    private ClusterSingletonProxySettings(
            String singletonName,
            Optional<String> role,
            Duration singletonIdentificationInterval,
            int bufferSize) {
        this.singletonName = singletonName;
        this.role = role;
        this.singletonIdentificationInterval = singletonIdentificationInterval;
        this.bufferSize = bufferSize;
    }

    /** 从系统默认配置 {@code tapio.cluster.singleton-proxy} 创建。 */
    public static ClusterSingletonProxySettings create(ActorSystem system) {
        return create(system.settings().config().getConfig("tapio.cluster.singleton-proxy"));
    }

    /** 从指定配置节创建。 */
    public static ClusterSingletonProxySettings create(Config config) {
        String rawRole = config.getString("role");
        Optional<String> role = rawRole.isEmpty() ? Optional.empty() : Optional.of(rawRole);
        return new ClusterSingletonProxySettings(
                config.getString("singleton-name"),
                role,
                config.getDuration("singleton-identification-interval"),
                config.getInt("buffer-size"));
    }

    public String singletonName() {
        return singletonName;
    }

    public Optional<String> role() {
        return role;
    }

    public Duration singletonIdentificationInterval() {
        return singletonIdentificationInterval;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public ClusterSingletonProxySettings withSingletonName(String name) {
        return new ClusterSingletonProxySettings(name, role, singletonIdentificationInterval, bufferSize);
    }

    public ClusterSingletonProxySettings withRole(String role) {
        Optional<String> r = (role == null || role.isEmpty()) ? Optional.empty() : Optional.of(role);
        return new ClusterSingletonProxySettings(singletonName, r, singletonIdentificationInterval, bufferSize);
    }

    public ClusterSingletonProxySettings withRole(Optional<String> role) {
        return new ClusterSingletonProxySettings(singletonName, role, singletonIdentificationInterval, bufferSize);
    }

    public ClusterSingletonProxySettings withSingletonIdentificationInterval(Duration interval) {
        return new ClusterSingletonProxySettings(singletonName, role, interval, bufferSize);
    }

    public ClusterSingletonProxySettings withBufferSize(int bufferSize) {
        return new ClusterSingletonProxySettings(singletonName, role, singletonIdentificationInterval, bufferSize);
    }
}
