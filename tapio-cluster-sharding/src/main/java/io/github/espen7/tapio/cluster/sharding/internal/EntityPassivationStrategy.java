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
package io.github.espen7.tapio.cluster.sharding.internal;

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.sharding.ClusterShardingSettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * INTERNAL API：实体钝化策略抽象与内置实现（支持空闲超时、LRU、MRU、LFU 与禁用）。
 */
@InternalApi
public abstract class EntityPassivationStrategy {

    /**
     * 根据配置构建 {@link EntityPassivationStrategy} 实例。
     */
    public static EntityPassivationStrategy create(ClusterShardingSettings settings) {
        Duration idleAfter = settings.passivateIdleEntityAfter();
        if (idleAfter == null || idleAfter.isZero() || idleAfter.isNegative()) {
            return Disabled.INSTANCE;
        }
        return new Idle(idleAfter);
    }

    /** 根据策略名称（{@code idle}/{@code lru}/{@code mru}/{@code lfu}/{@code off}）与上限构造策略。 */
    public static EntityPassivationStrategy create(String strategyName, int activeEntityLimit, Duration idleAfter) {
        if (strategyName == null) {
            return Disabled.INSTANCE;
        }
        return switch (strategyName.toLowerCase()) {
            case "lru" -> new LeastRecentlyUsed(activeEntityLimit, idleAfter);
            case "mru" -> new MostRecentlyUsed(activeEntityLimit, idleAfter);
            case "lfu" -> new LeastFrequentlyUsed(activeEntityLimit, idleAfter);
            case "idle" -> (idleAfter != null && !idleAfter.isZero() && !idleAfter.isNegative())
                    ? new Idle(idleAfter)
                    : Disabled.INSTANCE;
            default -> Disabled.INSTANCE;
        };
    }

    /** 通知策略某个实体刚收到消息，返回因超出容量需立即钝化的实体 ID 列表。 */
    public abstract List<String> entityTouched(String entityId);

    /** 通知策略某个实体已终止。 */
    public abstract void entityTerminated(String entityId);

    /** 返回定期检查空闲实体的间隔（若无需定期检查则返回 {@link Duration#ZERO}）。 */
    public abstract Duration interval();

    /** 定期检查并返回超时需要钝化的空闲实体 ID 列表。 */
    public abstract List<String> checkIdle();

    /** 禁用钝化。 */
    public static final class Disabled extends EntityPassivationStrategy {
        public static final Disabled INSTANCE = new Disabled();

        private Disabled() {
        }

        @Override
        public List<String> entityTouched(String entityId) {
            return List.of();
        }

        @Override
        public void entityTerminated(String entityId) {
        }

        @Override
        public Duration interval() {
            return Duration.ZERO;
        }

        @Override
        public List<String> checkIdle() {
            return List.of();
        }
    }

    /** 空闲超时钝化策略。 */
    public static class Idle extends EntityPassivationStrategy {
        private final Duration idleAfter;
        private final Map<String, Long> lastActivityNanos = new HashMap<>();

        public Idle(Duration idleAfter) {
            this.idleAfter = idleAfter;
        }

        @Override
        public List<String> entityTouched(String entityId) {
            lastActivityNanos.put(entityId, System.nanoTime());
            return List.of();
        }

        @Override
        public void entityTerminated(String entityId) {
            lastActivityNanos.remove(entityId);
        }

        @Override
        public Duration interval() {
            if (idleAfter == null || idleAfter.isZero() || idleAfter.isNegative()) {
                return Duration.ZERO;
            }
            Duration half = idleAfter.dividedBy(2);
            return half.toMillis() < 50 ? Duration.ofMillis(50) : half;
        }

        @Override
        public List<String> checkIdle() {
            if (idleAfter == null || idleAfter.isZero() || idleAfter.isNegative()) {
                return List.of();
            }
            long now = System.nanoTime();
            long threshold = idleAfter.toNanos();
            List<String> expired = new ArrayList<>();
            for (Map.Entry<String, Long> e : lastActivityNanos.entrySet()) {
                if (now - e.getValue() >= threshold) {
                    expired.add(e.getKey());
                }
            }
            return expired;
        }
    }

    /** 最近最少使用（LRU）容量淘汰策略。 */
    public static final class LeastRecentlyUsed extends Idle {
        private final int limit;
        private final LinkedHashMap<String, Boolean> lru = new LinkedHashMap<>(16, 0.75f, true);

        public LeastRecentlyUsed(int limit, Duration idleAfter) {
            super(idleAfter);
            this.limit = Math.max(1, limit);
        }

        @Override
        public List<String> entityTouched(String entityId) {
            super.entityTouched(entityId);
            lru.put(entityId, Boolean.TRUE);
            if (lru.size() <= limit) {
                return List.of();
            }
            List<String> passivate = new ArrayList<>();
            var it = lru.keySet().iterator();
            while (lru.size() > limit && it.hasNext()) {
                passivate.add(it.next());
                it.remove();
            }
            return passivate;
        }

        @Override
        public void entityTerminated(String entityId) {
            super.entityTerminated(entityId);
            lru.remove(entityId);
        }
    }

    /** 最近最多使用（MRU）容量淘汰策略。 */
    public static final class MostRecentlyUsed extends Idle {
        private final int limit;
        private final List<String> order = new ArrayList<>();

        public MostRecentlyUsed(int limit, Duration idleAfter) {
            super(idleAfter);
            this.limit = Math.max(1, limit);
        }

        @Override
        public List<String> entityTouched(String entityId) {
            super.entityTouched(entityId);
            order.remove(entityId);
            order.add(entityId);
            if (order.size() <= limit) {
                return List.of();
            }
            List<String> passivate = new ArrayList<>();
            while (order.size() > limit) {
                String victim = order.remove(order.size() - 2);
                passivate.add(victim);
            }
            return passivate;
        }

        @Override
        public void entityTerminated(String entityId) {
            super.entityTerminated(entityId);
            order.remove(entityId);
        }
    }

    /** 最不经常使用（LFU）容量淘汰策略。 */
    public static final class LeastFrequentlyUsed extends Idle {
        private final int limit;
        private final Map<String, Long> freq = new LinkedHashMap<>();

        public LeastFrequentlyUsed(int limit, Duration idleAfter) {
            super(idleAfter);
            this.limit = Math.max(1, limit);
        }

        @Override
        public List<String> entityTouched(String entityId) {
            super.entityTouched(entityId);
            freq.merge(entityId, 1L, Long::sum);
            if (freq.size() <= limit) {
                return List.of();
            }
            List<String> passivate = new ArrayList<>();
            while (freq.size() > limit) {
                String victim = null;
                long minFreq = Long.MAX_VALUE;
                for (Map.Entry<String, Long> e : freq.entrySet()) {
                    if (!e.getKey().equals(entityId) && e.getValue() < minFreq) {
                        minFreq = e.getValue();
                        victim = e.getKey();
                    }
                }
                if (victim == null) {
                    break;
                }
                freq.remove(victim);
                passivate.add(victim);
            }
            return passivate;
        }

        @Override
        public void entityTerminated(String entityId) {
            super.entityTerminated(entityId);
            freq.remove(entityId);
        }
    }
}
