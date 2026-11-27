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
package io.github.espen7.tapio.remote;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorIdentity;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Identify;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemotingIntegrationTest {

    static final class EchoActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(byte[].class, bytes -> getSender().tell(bytes, getSelf()))
                    .build();
        }
    }

    private static Config remoteConfig() {
        return ConfigFactory.parseString("""
                tapio {
                  actor {
                    provider = "remote"
                  }
                  remote.classic.netty.tcp {
                    hostname = "127.0.0.1"
                    port = 0
                  }
                }
                """);
    }

    @Test
    void twoSystemsCommunicateOverNettyTcp() throws Exception {
        ActorSystem nodeA = ActorSystem.create("nodeA", remoteConfig());
        ActorSystem nodeB = ActorSystem.create("nodeB", remoteConfig());
        try {
            Address addrA = BoundAddressesExtension.INSTANCE.get(nodeA).boundAddresses().iterator().next();
            Address addrB = BoundAddressesExtension.INSTANCE.get(nodeB).boundAddresses().iterator().next();
            assertNotNull(addrA.port());
            assertNotNull(addrB.port());

            nodeB.actorOf(Props.create(EchoActor.class), "echo");

            String remoteEchoPath = addrB + "/user/echo";
            ActorIdentity identity = (ActorIdentity) Patterns.ask(
                            nodeA.actorSelection(remoteEchoPath), new Identify("id-1".getBytes()), Duration.ofSeconds(5))
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            assertTrue(identity.getActorRef().isPresent());

            ActorRef remoteEcho = identity.getActorRef().get();
            assertFalse(((RemoteActorRef) remoteEcho).isLocal());

            byte[] payload = "hello-remote-tapio".getBytes();
            byte[] reply = (byte[]) Patterns.ask(remoteEcho, payload, Duration.ofSeconds(5))
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            assertEquals("hello-remote-tapio", new String(reply));
        } finally {
            nodeA.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
            nodeB.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void phiAccrualFailureDetectorDetectsTimeout() {
        AtomicLong now = new AtomicLong(1000L);
        PhiAccrualFailureDetector fd = new PhiAccrualFailureDetector(
                8.0,
                100,
                Duration.ofMillis(10),
                Duration.ofMillis(50),
                Duration.ofMillis(100),
                java.util.Optional.empty(),
                now::get);
        assertTrue(fd.isAvailable());
        assertFalse(fd.isMonitoring());

        fd.heartbeat();
        assertTrue(fd.isMonitoring());
        assertTrue(fd.isAvailable());

        now.addAndGet(100L);
        fd.heartbeat();
        assertTrue(fd.isAvailable());

        now.addAndGet(5000L);
        assertFalse(fd.isAvailable());
    }
}
