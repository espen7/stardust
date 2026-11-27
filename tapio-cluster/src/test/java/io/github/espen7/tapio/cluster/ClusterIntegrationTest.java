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
package io.github.espen7.tapio.cluster;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Props;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterIntegrationTest {

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

    private static Config clusterConfig() {
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
                }
                """);
    }

    @Test
    void twoNodeClusterFormsAndReachesUp() throws Exception {
        ActorSystem sys1 = ActorSystem.create("cluster-sys", clusterConfig());
        ActorSystem sys2 = ActorSystem.create("cluster-sys", clusterConfig());
        try {
            Cluster c1 = Cluster.get(sys1);
            Cluster c2 = Cluster.get(sys2);

            Set<Address> seenUp = ConcurrentHashMap.newKeySet();
            CountDownLatch upLatch = new CountDownLatch(1);
            var collector = sys1.actorOf(Props.create(
                    MemberUpCollector.class, () -> new MemberUpCollector(seenUp, upLatch)), "collector");
            c1.subscribe(collector, ClusterEvent.MemberUp.class);

            List<Address> seeds = List.of(c1.selfAddress(), c2.selfAddress());
            c1.joinSeedNodes(seeds);
            c2.joinSeedNodes(seeds);

            assertTrue(upLatch.await(10, TimeUnit.SECONDS), "Expected both nodes to reach Up");
            assertEquals(2, seenUp.size());
            assertTrue(seenUp.contains(c1.selfAddress()));
            assertTrue(seenUp.contains(c2.selfAddress()));
            assertEquals(MemberStatus.Up, c1.selfMember().status());
        } finally {
            sys1.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            sys2.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
