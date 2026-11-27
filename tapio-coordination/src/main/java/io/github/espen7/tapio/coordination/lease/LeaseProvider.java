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
import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 租约提供者扩展，根据配置路径中的 {@code lease-class} 动态实例化并缓存 {@link Lease}。
 */
public final class LeaseProvider implements Extension {

    /** 扩展 ID。 */
    public static final AbstractExtensionId<LeaseProvider> Id = new AbstractExtensionId<>() {
        @Override
        public LeaseProvider createExtension(ExtendedActorSystem system) {
            return new LeaseProvider(system);
        }
    };

    /**
     * 获取或创建关联的 {@link LeaseProvider} 扩展实例。
     */
    public static LeaseProvider get(ActorSystem system) {
        return Id.apply(system);
    }

    private record LeaseKey(String leaseName, String configPath, String ownerName) {
    }

    private final ExtendedActorSystem system;
    private final LoggingAdapter log;
    private final ConcurrentHashMap<LeaseKey, Lease> leases = new ConcurrentHashMap<>();

    private LeaseProvider(ExtendedActorSystem system) {
        this.system = system;
        this.log = Logging.getLogger(system, this, getClass());
    }

    /**
     * 获取或创建指定名称与配置路径的 {@link Lease} 实例。
     *
     * @param leaseName 租约资源名称
     * @param configPath 包含 {@code lease-class} 的配置路径
     * @param ownerName 当前持有者标识（通常为节点地址）
     * @return {@link Lease} 实例
     */
    public Lease getLease(String leaseName, String configPath, String ownerName) {
        LeaseKey key = new LeaseKey(leaseName, configPath, ownerName);
        return leases.computeIfAbsent(key, k -> instantiateLease(leaseName, configPath, ownerName));
    }

    private Lease instantiateLease(String leaseName, String configPath, String ownerName) {
        Config leaseConfig = system.settings().config()
                .getConfig(configPath)
                .withFallback(system.settings().config().getConfig("tapio.coordination.lease"));
        LeaseSettings settings = LeaseSettings.create(leaseConfig, leaseName, ownerName);
        String fqcn = settings.leaseConfig().getString("lease-class");
        if (fqcn == null || fqcn.isBlank()) {
            throw new LeaseException(
                    "Invalid lease configuration for leaseName [" + leaseName + "], configPath ["
                            + configPath + "]: lease-class must not be empty");
        }
        try {
            return system.dynamicAccess().createInstanceFor(
                    fqcn,
                    Lease.class,
                    List.of(
                            Map.entry(LeaseSettings.class, settings),
                            Map.entry(ExtendedActorSystem.class, system)));
        } catch (Throwable first) {
            try {
                return system.dynamicAccess().createInstanceFor(
                        fqcn,
                        Lease.class,
                        List.of(Map.entry(LeaseSettings.class, settings)));
            } catch (Throwable second) {
                log.error(second,
                        "Invalid lease configuration for leaseName [{}], configPath [{}] lease-class [{}]. "
                                + "The class must extend Lease and have constructor with LeaseSettings "
                                + "and optionally ExtendedActorSystem parameter.",
                        leaseName, configPath, fqcn);
                if (second instanceof RuntimeException re) {
                    throw re;
                }
                throw new LeaseException("Failed to instantiate lease [" + fqcn + "]", second);
            }
        }
    }
}
