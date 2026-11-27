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
package io.github.espen7.tapio.serialization;

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.setup.ActorSystemSetup;
import org.junit.jupiter.api.Test;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerializationTest {

    record CustomMessage(String text) {
    }

    static final class CustomStringSerializer extends SerializerWithStringManifest {
        @Override
        public int identifier() {
            return 1001;
        }

        @Override
        public String manifest(Object o) {
            return o instanceof CustomMessage ? "C" : "";
        }

        @Override
        public byte[] toBinary(Object o) {
            return ((CustomMessage) o).text().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
            if (!"C".equals(manifest)) {
                throw new NotSerializableException("Unknown manifest: " + manifest);
            }
            return new CustomMessage(new String(bytes, StandardCharsets.UTF_8));
        }
    }

    static final class DummyActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
    }

    @Test
    void builtInSerializersAndJavaDisabledByDefault() throws Exception {
        ActorSystem system = ActorSystem.create("ser-test");
        try {
            Serialization ser = SerializationExtension.INSTANCE.get(system);

            byte[] raw = "hello".getBytes(StandardCharsets.UTF_8);
            Serializer bytesSer = ser.findSerializerFor(raw);
            assertEquals(4, bytesSer.identifier());
            byte[] encoded = ser.serialize(raw);
            assertArrayEquals(raw, (byte[]) ser.deserialize(encoded, 4, ""));

            ByteBuffer buf = ByteBuffer.wrap(raw);
            assertArrayEquals(raw, (byte[]) ser.deserializeByteBuffer(buf, 4, ""));

            byte[] nullBytes = ser.serialize(null);
            assertEquals(0, nullBytes.length);
            assertNull(ser.deserialize(nullBytes, 0, ""));
            assertThrows(NotSerializableException.class, () -> ser.deserialize(new byte[] { 1 }, 0, ""));

            assertThrows(DisabledJavaSerializer.JavaSerializationException.class,
                    () -> ser.serialize(new java.util.Date(123456L)));
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void javaSerializerAndProgrammaticSetupWork() throws Exception {
        var config = ConfigFactory.parseString("tapio.actor.allow-java-serialization = on");
        SerializationSetup setup = SerializationSetup.create(
                sys -> List.of(SerializerDetails.create(
                        "custom", new CustomStringSerializer(), List.of(CustomMessage.class))));
        ActorSystem system = ActorSystem.create("ser-setup-test", config, ActorSystemSetup.create(setup));
        try {
            Serialization ser = SerializationExtension.INSTANCE.get(system);

            Serializable payload = new java.util.Date(987654L);
            byte[] javaBytes = ser.serialize(payload);
            assertEquals(payload, ser.deserialize(javaBytes, 1, ""));

            CustomMessage msg = new CustomMessage("tapio");
            Serializer customSer = ser.findSerializerFor(msg);
            assertEquals(1001, customSer.identifier());
            byte[] customBytes = ser.serialize(msg);
            assertEquals(msg, ser.deserialize(customBytes, 1001, Serializers.manifestFor(customSer, msg)));

            ActorRef ref = system.actorOf(Props.create(DummyActor.class), "dummy");
            String serializedPath = Serialization.withTransportInformation(
                    (ExtendedActorSystem) system, () -> Serialization.serializedActorPath(ref));
            assertTrue(serializedPath.contains("/user/dummy"));
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void nestedDeserializationEnforcesMaxDepth() throws Exception {
        assertEquals(0, NestedDeserialization.currentDepth());
        assertThrows(NotSerializableException.class, () -> NestedDeserialization.atNextLevel(1, () ->
                NestedDeserialization.atNextLevel(1, () -> "too-deep")));
        assertEquals(0, NestedDeserialization.currentDepth());
    }
}
