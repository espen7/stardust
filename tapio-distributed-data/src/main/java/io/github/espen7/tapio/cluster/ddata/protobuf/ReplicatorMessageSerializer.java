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
package io.github.espen7.tapio.cluster.ddata.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.cluster.UniqueAddress;
import io.github.espen7.tapio.cluster.ddata.LWWRegister;
import io.github.espen7.tapio.cluster.ddata.ORSet;
import io.github.espen7.tapio.cluster.ddata.ReplicatedData;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.VersionVector;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.SerializerWithStringManifest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 分布式数据内部消息与 CRDT 状态的序列化器。
 */
public class ReplicatorMessageSerializer extends SerializerWithStringManifest implements BaseSerializer {
    private static final String WRITE_MANIFEST = "W";
    private static final String WRITE_ACK_MANIFEST = "WA";
    private static final String READ_MANIFEST = "R";
    private static final String READ_RESULT_MANIFEST = "RR";
    private static final String GOSSIP_MANIFEST = "G";
    private static final String LWW_REGISTER_MANIFEST = "LWW";
    private static final String OR_SET_MANIFEST = "ORS";

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造分布式数据序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public ReplicatorMessageSerializer(ExtendedActorSystem system) {
        this.system = system;
        this.identifier = BaseSerializer.identifierFromConfig(getClass(), system);
    }

    @Override
    public ExtendedActorSystem system() {
        return system;
    }

    @Override
    public int identifier() {
        return identifier;
    }

    @Override
    public String manifest(Object o) {
        return switch (o) {
            case Replicator.Write ignored -> WRITE_MANIFEST;
            case Replicator.WriteAck ignored -> WRITE_ACK_MANIFEST;
            case Replicator.Read ignored -> READ_MANIFEST;
            case Replicator.ReadResult ignored -> READ_RESULT_MANIFEST;
            case Replicator.Gossip ignored -> GOSSIP_MANIFEST;
            case LWWRegister<?> ignored -> LWW_REGISTER_MANIFEST;
            case ORSet<?> ignored -> OR_SET_MANIFEST;
            default -> throw new IllegalArgumentException("Unsupported ddata message type: " + o.getClass().getName());
        };
    }

    @Override
    public byte[] toBinary(Object o) {
        try {
            return switch (o) {
                case Replicator.Write w -> encodeWrite(w);
                case Replicator.WriteAck wa -> encodeWriteAck(wa);
                case Replicator.Read r -> encodeRead(r);
                case Replicator.ReadResult rr -> encodeReadResult(rr);
                case Replicator.Gossip g -> encodeGossip(g);
                case LWWRegister<?> reg -> encodeLwwRegister(reg);
                case ORSet<?> set -> encodeOrSet(set);
                default -> throw new IllegalArgumentException(
                        "Unsupported ddata message type: " + o.getClass().getName());
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
        try {
            CodedInputStream in = CodedInputStream.newInstance(bytes);
            return switch (manifest) {
                case WRITE_MANIFEST -> decodeWrite(in);
                case WRITE_ACK_MANIFEST -> decodeWriteAck(in);
                case READ_MANIFEST -> decodeRead(in);
                case READ_RESULT_MANIFEST -> decodeReadResult(in);
                case GOSSIP_MANIFEST -> decodeGossip(in);
                case LWW_REGISTER_MANIFEST -> decodeLwwRegister(in);
                case OR_SET_MANIFEST -> decodeOrSet(in);
                default -> throw new NotSerializableException("Unknown ddata manifest: " + manifest);
            };
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException(
                    "Failed to deserialize ReplicatorMessage [" + manifest + "]: " + e.getMessage());
            nse.initCause(e);
            throw nse;
        }
    }

    private byte[] encodeWrite(Replicator.Write w) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, w.keyId());
        writeDataEnvelope(out, 2, w.envelope());
        out.writeInt64(3, w.correlationId());
        out.flush();
        return baos.toByteArray();
    }

    private Replicator.Write decodeWrite(CodedInputStream in) throws IOException, NotSerializableException {
        String keyId = "";
        Replicator.DataEnvelope envelope = null;
        long correlationId = 0L;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> keyId = in.readString();
                case 2 -> envelope = readDataEnvelope(in.readByteArray());
                case 3 -> correlationId = in.readInt64();
                default -> in.skipField(tag);
            }
        }
        return new Replicator.Write(keyId, envelope, correlationId);
    }

    private byte[] encodeWriteAck(Replicator.WriteAck wa) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeInt64(1, wa.correlationId());
        out.flush();
        return baos.toByteArray();
    }

    private Replicator.WriteAck decodeWriteAck(CodedInputStream in) throws IOException {
        long correlationId = 0L;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                correlationId = in.readInt64();
            } else {
                in.skipField(tag);
            }
        }
        return new Replicator.WriteAck(correlationId);
    }

    private byte[] encodeRead(Replicator.Read r) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, r.keyId());
        out.writeInt64(2, r.correlationId());
        out.flush();
        return baos.toByteArray();
    }

    private Replicator.Read decodeRead(CodedInputStream in) throws IOException {
        String keyId = "";
        long correlationId = 0L;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> keyId = in.readString();
                case 2 -> correlationId = in.readInt64();
                default -> in.skipField(tag);
            }
        }
        return new Replicator.Read(keyId, correlationId);
    }

    private byte[] encodeReadResult(Replicator.ReadResult rr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeInt64(1, rr.correlationId());
        if (rr.envelope().isPresent()) {
            writeDataEnvelope(out, 2, rr.envelope().get());
        }
        out.flush();
        return baos.toByteArray();
    }

    private Replicator.ReadResult decodeReadResult(CodedInputStream in) throws IOException, NotSerializableException {
        long correlationId = 0L;
        Replicator.DataEnvelope envelope = null;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> correlationId = in.readInt64();
                case 2 -> envelope = readDataEnvelope(in.readByteArray());
                default -> in.skipField(tag);
            }
        }
        return new Replicator.ReadResult(correlationId, Optional.ofNullable(envelope));
    }

    private byte[] encodeGossip(Replicator.Gossip g) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<String, Replicator.DataEnvelope> entry : g.updatedData().entrySet()) {
            ByteArrayOutputStream entryBaos = new ByteArrayOutputStream();
            CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBaos);
            entryOut.writeString(1, entry.getKey());
            writeDataEnvelope(entryOut, 2, entry.getValue());
            entryOut.flush();
            out.writeByteArray(1, entryBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private Replicator.Gossip decodeGossip(CodedInputStream in) throws IOException, NotSerializableException {
        Map<String, Replicator.DataEnvelope> updated = new HashMap<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                CodedInputStream entryIn = CodedInputStream.newInstance(in.readByteArray());
                String keyId = "";
                Replicator.DataEnvelope env = null;
                while (!entryIn.isAtEnd()) {
                    int entryTag = entryIn.readTag();
                    switch (entryTag >>> 3) {
                        case 1 -> keyId = entryIn.readString();
                        case 2 -> env = readDataEnvelope(entryIn.readByteArray());
                        default -> entryIn.skipField(entryTag);
                    }
                }
                if (env != null) {
                    updated.put(keyId, env);
                }
            } else {
                in.skipField(tag);
            }
        }
        return new Replicator.Gossip(updated);
    }

    private void writeDataEnvelope(CodedOutputStream out, int fieldNum, Replicator.DataEnvelope envelope)
            throws IOException {
        WireFormats.SerializedMessage sm = MessageSerializer.serialize(system, envelope.data());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream envOut = CodedOutputStream.newInstance(baos);
        sm.writePayloadTo(envOut);
        envOut.flush();
        out.writeByteArray(fieldNum, baos.toByteArray());
    }

    private Replicator.DataEnvelope readDataEnvelope(byte[] bytes) throws IOException, NotSerializableException {
        WireFormats.SerializedMessage sm = WireFormats.SerializedMessage.parsePayloadFrom(
                CodedInputStream.newInstance(bytes));
        ReplicatedData data = (ReplicatedData) MessageSerializer.deserialize(system, sm);
        return new Replicator.DataEnvelope(data);
    }

    private byte[] encodeLwwRegister(LWWRegister<?> reg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        writeUniqueAddress(out, 1, reg.updatedBy());
        out.writeInt64(2, reg.timestamp());
        writePayload(out, 3, reg.value());
        out.flush();
        return baos.toByteArray();
    }

    private LWWRegister<Object> decodeLwwRegister(CodedInputStream in) throws IOException, NotSerializableException {
        UniqueAddress node = null;
        long timestamp = 0L;
        Object value = null;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> node = readUniqueAddress(in.readByteArray());
                case 2 -> timestamp = in.readInt64();
                case 3 -> value = readPayload(in.readByteArray());
                default -> in.skipField(tag);
            }
        }
        return new LWWRegister<>(node, value, timestamp);
    }

    private byte[] encodeOrSet(ORSet<?> set) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        writeVersionVector(out, 1, set.vvector());
        for (Map.Entry<?, VersionVector> entry : set.elementsMap().entrySet()) {
            ByteArrayOutputStream entryBaos = new ByteArrayOutputStream();
            CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBaos);
            writePayload(entryOut, 1, entry.getKey());
            writeVersionVector(entryOut, 2, entry.getValue());
            entryOut.flush();
            out.writeByteArray(2, entryBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private ORSet<Object> decodeOrSet(CodedInputStream in) throws IOException, NotSerializableException {
        VersionVector vvector = VersionVector.EMPTY;
        Map<Object, VersionVector> elements = new HashMap<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> vvector = readVersionVector(in.readByteArray());
                case 2 -> {
                    CodedInputStream entryIn = CodedInputStream.newInstance(in.readByteArray());
                    Object elem = null;
                    VersionVector dot = VersionVector.EMPTY;
                    while (!entryIn.isAtEnd()) {
                        int entryTag = entryIn.readTag();
                        switch (entryTag >>> 3) {
                            case 1 -> elem = readPayload(entryIn.readByteArray());
                            case 2 -> dot = readVersionVector(entryIn.readByteArray());
                            default -> entryIn.skipField(entryTag);
                        }
                    }
                    if (elem != null) {
                        elements.put(elem, dot);
                    }
                }
                default -> in.skipField(tag);
            }
        }
        return new ORSet<>(elements, vvector);
    }

    private static void writeUniqueAddress(CodedOutputStream out, int fieldNum, UniqueAddress ua) throws IOException {
        ByteArrayOutputStream addrBaos = new ByteArrayOutputStream();
        CodedOutputStream addrOut = CodedOutputStream.newInstance(addrBaos);
        WireFormats.writeAddressData(addrOut, ua.address());
        addrOut.flush();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream uaOut = CodedOutputStream.newInstance(baos);
        uaOut.writeByteArray(1, addrBaos.toByteArray());
        uaOut.writeUInt64(2, ua.longUid());
        uaOut.flush();
        out.writeByteArray(fieldNum, baos.toByteArray());
    }

    private static UniqueAddress readUniqueAddress(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        Address address = null;
        long uid = 0L;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> address = WireFormats.readAddressData(CodedInputStream.newInstance(in.readByteArray()));
                case 2 -> uid = in.readUInt64();
                default -> in.skipField(tag);
            }
        }
        return new UniqueAddress(address, uid);
    }

    private static void writeVersionVector(CodedOutputStream out, int fieldNum, VersionVector vv) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream vvOut = CodedOutputStream.newInstance(baos);
        for (Map.Entry<UniqueAddress, Long> entry : vv.versions().entrySet()) {
            ByteArrayOutputStream entryBaos = new ByteArrayOutputStream();
            CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBaos);
            writeUniqueAddress(entryOut, 1, entry.getKey());
            entryOut.writeInt64(2, entry.getValue());
            entryOut.flush();
            vvOut.writeByteArray(1, entryBaos.toByteArray());
        }
        vvOut.flush();
        out.writeByteArray(fieldNum, baos.toByteArray());
    }

    private static VersionVector readVersionVector(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        TreeMap<UniqueAddress, Long> versions = new TreeMap<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                CodedInputStream entryIn = CodedInputStream.newInstance(in.readByteArray());
                UniqueAddress ua = null;
                long ver = 0L;
                while (!entryIn.isAtEnd()) {
                    int entryTag = entryIn.readTag();
                    switch (entryTag >>> 3) {
                        case 1 -> ua = readUniqueAddress(entryIn.readByteArray());
                        case 2 -> ver = entryIn.readInt64();
                        default -> entryIn.skipField(entryTag);
                    }
                }
                if (ua != null) {
                    versions.put(ua, ver);
                }
            } else {
                in.skipField(tag);
            }
        }
        return new VersionVector(versions);
    }

    private void writePayload(CodedOutputStream out, int fieldNum, Object value) throws IOException {
        WireFormats.SerializedMessage sm = MessageSerializer.serialize(system, value);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream pOut = CodedOutputStream.newInstance(baos);
        sm.writePayloadTo(pOut);
        pOut.flush();
        out.writeByteArray(fieldNum, baos.toByteArray());
    }

    private Object readPayload(byte[] bytes) throws IOException, NotSerializableException {
        WireFormats.SerializedMessage sm = WireFormats.SerializedMessage.parsePayloadFrom(
                CodedInputStream.newInstance(bytes));
        return MessageSerializer.deserialize(system, sm);
    }
}
