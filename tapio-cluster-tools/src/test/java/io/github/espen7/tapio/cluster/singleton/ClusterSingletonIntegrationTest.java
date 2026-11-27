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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterSingletonIntegrationTest {

    static final class EchoSingleton extends AbstractActor {
        private final Address address;

        EchoSingleton(Address address) {
            this.address = address;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, msg -> getSender().tell(msg + "@" + address, getSelf()))
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
                  cluster.singleton {
                    hand-over-retry-interval = 100ms
                  }
                  cluster.singleton-proxy {
                    singleton-identification-interval = 100ms
                  }
                }
                """);
    }

    @Test
    void singletonRoutesViaProxyAndHandsOverWhenOldestLeaves() throws Exception {
        ActorSystem sys1 = ActorSystem.create("singleton-sys", clusterConfig());
        ActorSystem sys2 = ActorSystem.create("singleton-sys", clusterConfig());
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

            ClusterSingletonManagerSettings mgrSettings1 = ClusterSingletonManagerSettings.create(sys1)
                    .withRemovalMargin(Duration.ofMillis(200));
            ClusterSingletonManagerSettings mgrSettings2 = ClusterSingletonManagerSettings.create(sys2)
                    .withRemovalMargin(Duration.ofMillis(200));

            sys1.actorOf(ClusterSingletonManager.props(
                    Props.create(EchoSingleton.class, () -> new EchoSingleton(c1.selfAddress())),
                    PoisonPill.getInstance(),
                    mgrSettings1), "echoManager");
            sys2.actorOf(ClusterSingletonManager.props(
                    Props.create(EchoSingleton.class, () -> new EchoSingleton(c2.selfAddress())),
                    PoisonPill.getInstance(),
                    mgrSettings2), "echoManager");

            ActorRef proxy2 = sys2.actorOf(ClusterSingletonProxy.props(
                    "/user/echoManager",
                    ClusterSingletonProxySettings.create(sys2)), "echoProxy");

            BlockingQueue<String> replies = new LinkedBlockingQueue<>();
            ActorRef replyCollector = sys2.actorOf(Props.create(
                    ReplyCollector.class, () -> new ReplyCollector(replies)), "replyCollector");

            proxy2.tell("hello", replyCollector);
            String firstReply = replies.poll(10, TimeUnit.SECONDS);
            assertNotNull(firstReply, "Expected reply from singleton");
            assertEquals("hello@" + c1.selfAddress(), firstReply);

            c1.leave(c1.selfAddress());

            String secondReply = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            while (System.nanoTime() < deadline) {
                proxy2.tell("afterLeave", replyCollector);
                String reply = replies.poll(300, TimeUnit.MILLISECONDS);
                if (("afterLeave@" + c2.selfAddress()).equals(reply)) {
                    secondReply = reply;
                    break;
                }
            }
            assertEquals("afterLeave@" + c2.selfAddress(), secondReply);
        } finally {
            sys1.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            sys2.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
