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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.sharding.external.ExternalShardAllocation;
import io.github.espen7.tapio.cluster.sharding.external.ExternalShardAllocationStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingStrategiesTest {

    static final class DummyActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
    }

    static final class UpWaiter extends AbstractActor {
        private final java.util.concurrent.CountDownLatch latch;

        UpWaiter(java.util.concurrent.CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ClusterEvent.MemberUp.class, ev -> latch.countDown())
                    .build();
        }
    }

    @Test
    void shardingQueriesFormatsMetadata() {
        var result = new ShardingQueries.ShardsQueryResult<>(
                Set.of("s3"),
                List.of("ok1", "ok2"),
                3,
                Duration.ofSeconds(2));
        assertEquals(3, result.queried());
        assertTrue(result.toString().contains("[2] responsive, [1] failed"));
    }

    @Test
    void consistentHashingAndExternalAllocationWork() throws Exception {
        var config = ConfigFactory.parseString("""
                tapio {
                  actor.provider = "cluster"
                  remote.classic.netty.tcp {
                    hostname = "127.0.0.1"
                    port = 0
                  }
                }
                """);
        ActorSystem sys = ActorSystem.create("strategy-sys", config);
        try {
            Cluster c = Cluster.get(sys);
            var upLatch = new java.util.concurrent.CountDownLatch(1);
            var waiter = sys.actorOf(Props.create(UpWaiter.class, () -> new UpWaiter(upLatch)), "upWaiter");
            c.subscribe(waiter, ClusterEvent.MemberUp.class);
            c.join(c.selfAddress());
            assertTrue(upLatch.await(5, TimeUnit.SECONDS));

            ActorRef dummyRegion = sys.actorOf(Props.create(DummyActor.class, DummyActor::new), "region1");
            ConsistentHashingShardAllocationStrategy ch = new ConsistentHashingShardAllocationStrategy(1);
            ch.start(sys);
            ActorRef chosen = ch.allocateShard(dummyRegion, "shard-0", Map.of(dummyRegion, List.of()))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(dummyRegion, chosen);

            ExternalShardAllocationStrategy ext = ExternalShardAllocationStrategy.create(sys, "orders");
            ExternalShardAllocation.get(sys).clientFor("orders")
                    .updateShardLocation("shard-1", c.selfAddress())
                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
            ActorRef extChosen = ext.allocateShard(dummyRegion, "shard-1", Map.of(dummyRegion, List.of()))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(dummyRegion, extChosen);
        } finally {
            sys.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
