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
package io.github.espen7.tapio.cluster.sbr;

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.annotation.InternalApi;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * INTERNAL API：Split Brain Resolver 配置解析与策略工厂。
 */
@InternalApi
public final class SplitBrainResolverSettings {

    public static final String KEEP_MAJORITY_NAME = "keep-majority";
    public static final String LEASE_MAJORITY_NAME = "lease-majority";
    public static final String STATIC_QUORUM_NAME = "static-quorum";
    public static final String KEEP_OLDEST_NAME = "keep-oldest";
    public static final String DOWN_ALL_NAME = "down-all";

    private static final Set<String> ALL_STRATEGY_NAMES = Set.of(
            KEEP_MAJORITY_NAME,
            LEASE_MAJORITY_NAME,
            STATIC_QUORUM_NAME,
            KEEP_OLDEST_NAME,
            DOWN_ALL_NAME);

    private final Duration downingStableAfter;
    private final String downingStrategy;
    private final Config sbrConfig;

    public SplitBrainResolverSettings(Config config) {
        this.sbrConfig = config.getConfig("tapio.cluster.split-brain-resolver");
        this.downingStableAfter = sbrConfig.getDuration("stable-after");
        if (downingStableAfter.isNegative()) {
            throw new ConfigurationException("stable-after must be >= 0s");
        }
        String strategy = sbrConfig.getString("active-strategy").toLowerCase(Locale.ROOT);
        if (!ALL_STRATEGY_NAMES.contains(strategy)) {
            throw new ConfigurationException(
                    "Unknown downing strategy [" + strategy + "]. Select one of " + ALL_STRATEGY_NAMES);
        }
        this.downingStrategy = strategy;
    }

    public Duration downingStableAfter() {
        return downingStableAfter;
    }

    public String downingStrategyName() {
        return downingStrategy;
    }

    /**
     * 根据配置创建对应的 {@link DowningStrategy} 实例。
     */
    public DowningStrategy createStrategy() {
        return switch (downingStrategy) {
            case KEEP_MAJORITY_NAME -> new KeepMajority(readRole(sbrConfig.getConfig(KEEP_MAJORITY_NAME)));
            case STATIC_QUORUM_NAME -> {
                Config c = sbrConfig.getConfig(STATIC_QUORUM_NAME);
                yield new StaticQuorum(c.getInt("quorum-size"), readRole(c));
            }
            case KEEP_OLDEST_NAME -> {
                Config c = sbrConfig.getConfig(KEEP_OLDEST_NAME);
                yield new KeepOldest(c.getBoolean("down-if-alone"), readRole(c));
            }
            case DOWN_ALL_NAME -> new DownAllNodes();
            case LEASE_MAJORITY_NAME -> {
                Config c = sbrConfig.getConfig(LEASE_MAJORITY_NAME);
                String impl = c.getString("lease-implementation");
                if (impl == null || impl.isBlank()) {
                    throw new ConfigurationException(
                            "SBR strategy [lease-majority] requires tapio.cluster.split-brain-resolver.lease-majority.lease-implementation");
                }
                yield new LeaseMajority(impl, c.getDuration("acquire-lease-delay-for-minority"), readRole(c));
            }
            default -> throw new ConfigurationException("Unknown downing strategy [" + downingStrategy + "]");
        };
    }

    private static Optional<String> readRole(Config c) {
        String r = c.getString("role");
        return (r == null || r.isEmpty()) ? Optional.empty() : Optional.of(r);
    }
}
