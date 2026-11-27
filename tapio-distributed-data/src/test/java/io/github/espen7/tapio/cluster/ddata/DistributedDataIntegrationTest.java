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
package io.github.espen7.tapio.cluster.ddata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedDataIntegrationTest {

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

    static final class DDataSubscriber extends AbstractActor {
        private final BlockingQueue<Object> messages;

        DDataSubscriber(BlockingQueue<Object> messages) {
            this.messages = messages;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(messages::offer)
                    .build();
        }
    }

    private static Config ddataConfig() {
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
                  cluster.distributed-data {
                    gossip-interval = 100ms
                  }
                }
                """);
    }

    @Test
    @SuppressWarnings("unchecked")
    void replicatesOrSetAndLwwRegisterAcrossNodes() throws Exception {
        ActorSystem sys1 = ActorSystem.create("ddata-sys", ddataConfig());
        ActorSystem sys2 = ActorSystem.create("ddata-sys", ddataConfig());
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

            DistributedData dd1 = DistributedData.get(sys1);
            DistributedData dd2 = DistributedData.get(sys2);

            ORSetKey<String> setKey = ORSetKey.create("items");
            LWWRegisterKey<String> regKey = LWWRegisterKey.create("leaderTag");

            BlockingQueue<Object> events2 = new LinkedBlockingQueue<>();
            ActorRef sub2 = sys2.actorOf(Props.create(
                    DDataSubscriber.class, () -> new DDataSubscriber(events2)), "sub2");
            dd2.replicator().tell(new Replicator.Subscribe<>(setKey, sub2), ActorRef.noSender);

            BlockingQueue<Object> replies1 = new LinkedBlockingQueue<>();
            ActorRef reply1 = sys1.actorOf(Props.create(
                    DDataSubscriber.class, () -> new DDataSubscriber(replies1)), "reply1");

            dd1.replicator().tell(new Replicator.Update<>(
                    setKey,
                    ORSet.create(),
                    new Replicator.WriteMajority(Duration.ofSeconds(5)),
                    Optional.of("add-alpha"),
                    s -> s.add(dd1.selfUniqueAddress(), "alpha")), reply1);

            Object updateResp = replies1.poll(5, TimeUnit.SECONDS);
            assertTrue(updateResp instanceof Replicator.UpdateSuccess<?>, "Expected UpdateSuccess, got " + updateResp);

            boolean observedAlpha = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                Object ev = events2.poll(500, TimeUnit.MILLISECONDS);
                if (ev instanceof Replicator.Changed<?> changed && changed.key().equals(setKey)) {
                    ORSet<String> replicated = (ORSet<String>) changed.dataValue();
                    if (replicated.contains("alpha")) {
                        observedAlpha = true;
                        break;
                    }
                }
            }
            assertTrue(observedAlpha, "Expected node 2 to observe replicated ORSet element");

            dd1.replicator().tell(new Replicator.Update<>(
                    regKey,
                    LWWRegister.create(dd1.selfUniqueAddress(), "v1"),
                    new Replicator.WriteMajority(Duration.ofSeconds(5)),
                    Optional.empty(),
                    r -> r.withValue(dd1.selfUniqueAddress(), "v2")), reply1);

            Object regUpdateResp = replies1.poll(5, TimeUnit.SECONDS);
            assertTrue(regUpdateResp instanceof Replicator.UpdateSuccess<?>);

            dd2.replicator().tell(new Replicator.Get<>(
                    regKey,
                    new Replicator.ReadMajority(Duration.ofSeconds(5)),
                    Optional.of("read-reg")), sub2);

            String observedReg = null;
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Object ev = events2.poll(500, TimeUnit.MILLISECONDS);
                if (ev instanceof Replicator.GetSuccess<?> gs && gs.key().equals(regKey)) {
                    observedReg = ((LWWRegister<String>) gs.dataValue()).value();
                    break;
                }
            }
            assertEquals("v2", observedReg);
        } finally {
            sys1.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            sys2.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
