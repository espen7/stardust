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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.ActorSystem;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseProviderTest {

    public static final class InMemoryTestLease extends Lease {
        private final AtomicBoolean held = new AtomicBoolean(false);

        public InMemoryTestLease(LeaseSettings settings) {
            super(settings);
        }

        @Override
        public CompletionStage<Boolean> acquire() {
            return CompletableFuture.completedFuture(held.compareAndSet(false, true));
        }

        @Override
        public CompletionStage<Boolean> acquire(Consumer<Optional<Throwable>> leaseLostCallback) {
            return acquire();
        }

        @Override
        public CompletionStage<Boolean> release() {
            return CompletableFuture.completedFuture(held.compareAndSet(true, false));
        }

        @Override
        public boolean checkLease() {
            return held.get();
        }
    }

    @Test
    void loadsCachesAndAcquiresConfiguredLease() throws Exception {
        var config = ConfigFactory.parseString("""
                tapio.test-lease {
                  lease-class = "io.github.espen7.tapio.coordination.lease.LeaseProviderTest$InMemoryTestLease"
                }
                """);
        ActorSystem system = ActorSystem.create("lease-test", config);
        try {
            LeaseProvider provider = LeaseProvider.get(system);
            Lease l1 = provider.getLease("my-lease", "tapio.test-lease", "node-1");
            Lease l2 = provider.getLease("my-lease", "tapio.test-lease", "node-1");
            assertSame(l1, l2);

            assertFalse(l1.checkLease());
            assertTrue(l1.acquire().toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertTrue(l1.checkLease());
            assertFalse(l1.acquire().toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertTrue(l1.release().toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertFalse(l1.checkLease());

            assertThrows(LeaseException.class,
                    () -> provider.getLease("bad", "tapio.coordination.lease", "node-1"));
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
