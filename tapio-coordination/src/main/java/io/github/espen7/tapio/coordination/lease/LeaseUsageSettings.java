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
package io.github.espen7.tapio.coordination.lease;

import java.time.Duration;
import java.util.Objects;

/**
 * 集群组件（Singleton / Sharding）启用分布式租约时的使用配置。
 */
public final class LeaseUsageSettings {

    private final String leaseImplementation;
    private final Duration leaseRetryInterval;
    private final String leaseName;

    public LeaseUsageSettings(String leaseImplementation, Duration leaseRetryInterval) {
        this(leaseImplementation, leaseRetryInterval, "");
    }

    public LeaseUsageSettings(String leaseImplementation, Duration leaseRetryInterval, String leaseName) {
        this.leaseImplementation = Objects.requireNonNull(leaseImplementation, "leaseImplementation");
        this.leaseRetryInterval = Objects.requireNonNull(leaseRetryInterval, "leaseRetryInterval");
        this.leaseName = Objects.requireNonNull(leaseName, "leaseName");
    }

    public String leaseImplementation() {
        return leaseImplementation;
    }

    public Duration getLeaseRetryInterval() {
        return leaseRetryInterval;
    }

    public String getLeaseName() {
        return leaseName;
    }

    public LeaseUsageSettings withLeaseName(String name) {
        return new LeaseUsageSettings(leaseImplementation, leaseRetryInterval, name);
    }

    @Override
    public String toString() {
        return leaseName.isEmpty()
                ? "LeaseUsageSettings(" + leaseImplementation + ", " + leaseRetryInterval + ")"
                : "LeaseUsageSettings(" + leaseImplementation + ", " + leaseRetryInterval + ", " + leaseName + ")";
    }
}
