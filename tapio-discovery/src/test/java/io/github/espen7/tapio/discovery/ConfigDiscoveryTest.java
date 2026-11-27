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
package io.github.espen7.tapio.discovery;

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.ActorSystem;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigDiscoveryTest {

    @Test
    void configDiscoveryResolvesConfiguredEndpoints() throws Exception {
        var config = ConfigFactory.parseString("""
                tapio.discovery {
                  method = "config"
                  config.services {
                    my-svc {
                      endpoints = [
                        { host = "127.0.0.1", port = 8558 },
                        { host = "127.0.0.2", port = 8559 }
                      ]
                    }
                  }
                }
                """);
        ActorSystem system = ActorSystem.create("discovery-test", config);
        try {
            ServiceDiscovery sd = Discovery.get(system).discovery();
            ServiceDiscovery.Resolved resolved = sd.lookup("my-svc", Duration.ofSeconds(3))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals("my-svc", resolved.serviceName());
            assertEquals(2, resolved.addresses().size());
            assertEquals("127.0.0.1", resolved.addresses().get(0).host());
            assertEquals(Optional.of(8558), resolved.addresses().get(0).port());
            assertEquals("127.0.0.2", resolved.addresses().get(1).host());
            assertEquals(Optional.of(8559), resolved.addresses().get(1).port());
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
