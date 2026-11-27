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
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.coordination.lease.LeaseUsageSettings;

import java.time.Duration;
import java.util.Optional;

/**
 * {@link ClusterSingletonManager} 配置参数。
 */
public final class ClusterSingletonManagerSettings {
    private final String singletonName;
    private final Optional<String> role;
    private final Duration removalMargin;
    private final Duration handOverRetryInterval;
    private final Optional<LeaseUsageSettings> leaseUsageSettings;

    private ClusterSingletonManagerSettings(
            String singletonName,
            Optional<String> role,
            Duration removalMargin,
            Duration handOverRetryInterval,
            Optional<LeaseUsageSettings> leaseUsageSettings) {
        this.singletonName = singletonName;
        this.role = role;
        this.removalMargin = removalMargin;
        this.handOverRetryInterval = handOverRetryInterval;
        this.leaseUsageSettings = leaseUsageSettings;
    }

    /** 从系统默认配置 {@code tapio.cluster.singleton} 创建。 */
    public static ClusterSingletonManagerSettings create(ActorSystem system) {
        return create(system.settings().config().getConfig("tapio.cluster.singleton"))
                .withRemovalMargin(Cluster.get(system).settings().DownRemovalMargin);
    }

    /** 从指定配置节创建。 */
    public static ClusterSingletonManagerSettings create(Config config) {
        String rawRole = config.getString("role");
        Optional<String> role = rawRole.isEmpty() ? Optional.empty() : Optional.of(rawRole);
        Optional<LeaseUsageSettings> lease = Optional.empty();
        if (config.hasPath("use-lease")) {
            String leaseImpl = config.getString("use-lease");
            if (leaseImpl != null && !leaseImpl.isBlank()) {
                Duration retry = config.hasPath("lease-retry-interval")
                        ? config.getDuration("lease-retry-interval")
                        : Duration.ofSeconds(5);
                lease = Optional.of(new LeaseUsageSettings(leaseImpl, retry));
            }
        }
        return new ClusterSingletonManagerSettings(
                config.getString("singleton-name"),
                role,
                Duration.ZERO,
                config.getDuration("hand-over-retry-interval"),
                lease);
    }

    public String singletonName() {
        return singletonName;
    }

    public Optional<String> role() {
        return role;
    }

    public Duration removalMargin() {
        return removalMargin;
    }

    public Duration handOverRetryInterval() {
        return handOverRetryInterval;
    }

    public Optional<LeaseUsageSettings> leaseUsageSettings() {
        return leaseUsageSettings;
    }

    public ClusterSingletonManagerSettings withSingletonName(String name) {
        return new ClusterSingletonManagerSettings(
                name, role, removalMargin, handOverRetryInterval, leaseUsageSettings);
    }

    public ClusterSingletonManagerSettings withRole(String role) {
        Optional<String> r = (role == null || role.isEmpty()) ? Optional.empty() : Optional.of(role);
        return new ClusterSingletonManagerSettings(
                singletonName, r, removalMargin, handOverRetryInterval, leaseUsageSettings);
    }

    public ClusterSingletonManagerSettings withRole(Optional<String> role) {
        return new ClusterSingletonManagerSettings(
                singletonName, role, removalMargin, handOverRetryInterval, leaseUsageSettings);
    }

    public ClusterSingletonManagerSettings withRemovalMargin(Duration removalMargin) {
        return new ClusterSingletonManagerSettings(
                singletonName, role, removalMargin, handOverRetryInterval, leaseUsageSettings);
    }

    public ClusterSingletonManagerSettings withHandOverRetryInterval(Duration interval) {
        return new ClusterSingletonManagerSettings(
                singletonName, role, removalMargin, interval, leaseUsageSettings);
    }

    public ClusterSingletonManagerSettings withLeaseUsageSettings(LeaseUsageSettings leaseUsageSettings) {
        return new ClusterSingletonManagerSettings(
                singletonName, role, removalMargin, handOverRetryInterval, Optional.ofNullable(leaseUsageSettings));
    }
}
