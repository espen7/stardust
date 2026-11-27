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

import com.typesafe.config.Config;

import java.util.Objects;

/**
 * 单个租约实例的构造配置。
 */
public final class LeaseSettings {

    private final String leaseName;
    private final String ownerName;
    private final TimeoutSettings timeoutSettings;
    private final Config leaseConfig;

    public LeaseSettings(
            String leaseName,
            String ownerName,
            TimeoutSettings timeoutSettings,
            Config leaseConfig) {
        this.leaseName = Objects.requireNonNull(leaseName, "leaseName");
        this.ownerName = Objects.requireNonNull(ownerName, "ownerName");
        this.timeoutSettings = Objects.requireNonNull(timeoutSettings, "timeoutSettings");
        this.leaseConfig = Objects.requireNonNull(leaseConfig, "leaseConfig");
    }

    public static LeaseSettings create(Config config, String leaseName, String ownerName) {
        return new LeaseSettings(leaseName, ownerName, TimeoutSettings.create(config), config);
    }

    public String leaseName() {
        return leaseName;
    }

    public String ownerName() {
        return ownerName;
    }

    public TimeoutSettings timeoutSettings() {
        return timeoutSettings;
    }

    public Config leaseConfig() {
        return leaseConfig;
    }

    public LeaseSettings withTimeoutSettings(TimeoutSettings timeoutSettings) {
        return new LeaseSettings(leaseName, ownerName, timeoutSettings, leaseConfig);
    }

    @Override
    public String toString() {
        return "LeaseSettings(" + leaseName + ", " + ownerName + ", " + timeoutSettings + ")";
    }
}
