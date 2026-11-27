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
package io.github.espen7.tapio.cluster.sharding;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.PoisonPill;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterShardingIntegrationTest {

    static final class CounterEntity extends AbstractActor {
        private int count = 0;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, cmd -> {
                        if (cmd.startsWith("inc:")) {
                            count += 1;
                            getSender().tell("count:" + count, getSelf());
                        } else if (cmd.startsWith("get:")) {
                            getSender().tell("count:" + count, getSelf());
                        } else if (cmd.startsWith("passivate:")) {
                            getContext().getParent().tell(
                                    new ShardRegion.Passivate(PoisonPill.getInstance()), getSelf());
                        }
                    })
                    .build();
        }
    }

    static final class ReplyCollector extends AbstractActor {
        private final BlockingQueue<String> replies;

        ReplyCollector(BlockingQueue<String> replies) {
            this.replies = replies;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, replies::offer)
                    .build();
        }
    }

    static final class MemberUpCollector extends AbstractActor {
        private final Set<Address> upAddresses;
        private final CountDownLatch latch;

        MemberUpCollector(Set<Address> upAddresses, CountDownLatch latch) {
            this.upAddresses = upAddresses;
            this.latch = latch;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ClusterEvent.MemberUp.class, ev -> {
                        if (upAddresses.add(ev.member().address()) && upAddresses.size() >= 2) {
                            latch.countDown();
                        }
                    })
                    .build();
        }
    }

    static final class StringExtractor extends ShardRegion.HashCodeMessageExtractor {
        StringExtractor() {
            super(10);
        }

        @Override
        public String entityId(Object message) {
            if (message instanceof String s) {
                int idx = s.indexOf(':');
                if (idx > 0) {
                    return s.substring(idx + 1);
                }
            }
            return null;
        }
    }

    private static Config shardingConfig() {
        return ConfigFactory.parseString("""
                tapio {
                  actor {
                    provider = "cluster"
                  }
                  remote.classic.netty.tcp {
                    hostname = "127.0.0.1"
                    port = 0
                  }
                  cluster {
                    gossip-interval = 100ms
                    leader-actions-interval = 100ms
                    unreachable-nodes-reaper-interval = 100ms
                  }
                  cluster.singleton {
                    hand-over-retry-interval = 100ms
                  }
                  cluster.distributed-data {
                    gossip-interval = 100ms
                  }
                  cluster.sharding {
                    retry-interval = 100ms
                    rebalance-interval = 300ms
                    waiting-for-state-timeout = 500ms
                    updating-state-timeout = 500ms
                  }
                }
                """);
    }

    @Test
    void rejectsPersistenceStateStoreMode() {
        ActorSystem sys = ActorSystem.create("sharding-reject-sys", shardingConfig());
        try {
            ClusterShardingSettings settings = ClusterShardingSettings.create(sys);
            assertThrows(IllegalArgumentException.class,
                    () -> settings.withStateStoreMode(ClusterShardingSettings.StateStoreModePersistence));
        } finally {
            try {
                sys.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void twoNodeShardingRoutesAndPassivatesEntities() throws Exception {
        ActorSystem sys1 = ActorSystem.create("sharding-sys", shardingConfig());
        ActorSystem sys2 = ActorSystem.create("sharding-sys", shardingConfig());
        try {
            Cluster c1 = Cluster.get(sys1);
            Cluster c2 = Cluster.get(sys2);

            Set<Address> seenUp = ConcurrentHashMap.newKeySet();
            CountDownLatch upLatch = new CountDownLatch(1);
            ActorRef upCollector = sys1.actorOf(Props.create(
                    MemberUpCollector.class, () -> new MemberUpCollector(seenUp, upLatch)), "upCollector");
            c1.subscribe(upCollector, ClusterEvent.MemberUp.class);

            List<Address> seeds = List.of(c1.selfAddress(), c2.selfAddress());
            c1.joinSeedNodes(seeds);
            c2.joinSeedNodes(seeds);
            assertTrue(upLatch.await(10, TimeUnit.SECONDS), "Cluster did not reach Up");

            ClusterShardingSettings settings1 = ClusterShardingSettings.create(sys1)
                    .withRememberEntities(true);
            ClusterShardingSettings settings2 = ClusterShardingSettings.create(sys2)
                    .withRememberEntities(true);

            ActorRef region1 = ClusterSharding.get(sys1).start(
                    "counter",
                    Props.create(CounterEntity.class, CounterEntity::new),
                    settings1,
                    new StringExtractor());
            ActorRef region2 = ClusterSharding.get(sys2).start(
                    "counter",
                    Props.create(CounterEntity.class, CounterEntity::new),
                    settings2,
                    new StringExtractor());

            BlockingQueue<String> replies = new LinkedBlockingQueue<>();
            ActorRef collector = sys2.actorOf(Props.create(
                    ReplyCollector.class, () -> new ReplyCollector(replies)), "replyCollector");

            region1.tell("inc:entity-a", collector);
            assertEquals("count:1", replies.poll(10, TimeUnit.SECONDS));

            region2.tell("inc:entity-a", collector);
            assertEquals("count:2", replies.poll(10, TimeUnit.SECONDS));

            region2.tell("get:entity-a", collector);
            assertEquals("count:2", replies.poll(10, TimeUnit.SECONDS));

            region1.tell("passivate:entity-a", ActorRef.noSender);

            String afterPassivate = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                region2.tell("get:entity-a", collector);
                String r = replies.poll(300, TimeUnit.MILLISECONDS);
                if ("count:0".equals(r)) {
                    afterPassivate = r;
                    break;
                }
            }
            assertEquals("count:0", afterPassivate, "Expected passivated entity to restart with fresh count");
        } finally {
            sys1.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            sys2.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void roleScopedShardingAndCrossRoleProxyWork() throws Exception {
        Config homeCfg = ConfigFactory.parseString("tapio.cluster.roles = [\"home\"]").withFallback(shardingConfig());
        Config gateCfg = ConfigFactory.parseString("tapio.cluster.roles = [\"gate\"]").withFallback(shardingConfig());

        ActorSystem homeSys = ActorSystem.create("role-sharding-sys", homeCfg);
        ActorSystem gateSys = ActorSystem.create("role-sharding-sys", gateCfg);
        try {
            Cluster cHome = Cluster.get(homeSys);
            Cluster cGate = Cluster.get(gateSys);

            Set<Address> seenUp = ConcurrentHashMap.newKeySet();
            CountDownLatch upLatch = new CountDownLatch(1);
            ActorRef upCollector = homeSys.actorOf(Props.create(
                    MemberUpCollector.class, () -> new MemberUpCollector(seenUp, upLatch)), "upCollector");
            cHome.subscribe(upCollector, ClusterEvent.MemberUp.class);

            List<Address> seeds = List.of(cHome.selfAddress(), cGate.selfAddress());
            cHome.joinSeedNodes(seeds);
            cGate.joinSeedNodes(seeds);
            assertTrue(upLatch.await(10, TimeUnit.SECONDS), "Cluster did not reach Up");

            ClusterShardingSettings homeSettings = ClusterShardingSettings.create(homeSys).withRole("home");
            ClusterSharding.get(homeSys).start(
                    "player",
                    Props.create(CounterEntity.class, CounterEntity::new),
                    homeSettings,
                    new StringExtractor());

            ActorRef gateProxy = ClusterSharding.get(gateSys).startProxy(
                    "player",
                    java.util.Optional.of("home"),
                    new StringExtractor());

            BlockingQueue<String> replies = new LinkedBlockingQueue<>();
            ActorRef collector = gateSys.actorOf(Props.create(
                    ReplyCollector.class, () -> new ReplyCollector(replies)), "replyCollector");

            gateProxy.tell("inc:p1", collector);
            assertEquals("count:1", replies.poll(10, TimeUnit.SECONDS));
        } finally {
            homeSys.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            gateSys.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
