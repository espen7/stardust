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
package io.github.espen7.tapio.remote.serialization;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.ActorIdentity;
import io.github.espen7.tapio.actor.ActorInitializationException;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Identify;
import io.github.espen7.tapio.actor.Kill;
import io.github.espen7.tapio.actor.LocalScope;
import io.github.espen7.tapio.actor.PoisonPill;
import io.github.espen7.tapio.actor.Status;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.RemoteScope;
import io.github.espen7.tapio.remote.RemoteWatcher;
import io.github.espen7.tapio.remote.UniqueAddress;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.Serialization;
import io.github.espen7.tapio.serialization.SerializerWithStringManifest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * 常用内置消息、引用、状态与异常的 Protobuf 序列化器。
 */
public class MiscMessageSerializer extends SerializerWithStringManifest implements BaseSerializer {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final String IDENTIFY_MANIFEST = "A";
    private static final String ACTOR_IDENTITY_MANIFEST = "B";
    private static final String STATUS_SUCCESS_MANIFEST = "D";
    private static final String STATUS_FAILURE_MANIFEST = "E";
    private static final String THROWABLE_MANIFEST = "F";
    private static final String ACTOR_REF_MANIFEST = "G";
    private static final String OPTIONAL_MANIFEST = "H";
    private static final String POISON_PILL_MANIFEST = "P";
    private static final String KILL_MANIFEST = "K";
    private static final String REMOTE_WATCHER_HB_MANIFEST = "RWHB";
    private static final String DONE_MANIFEST = "DONE";
    private static final String ADDRESS_MANIFEST = "AD";
    private static final String UNIQUE_ADDRESS_MANIFEST = "UD";
    private static final String REMOTE_WATCHER_HB_RESP_MANIFEST = "RWHR";
    private static final String LOCAL_SCOPE_MANIFEST = "LS";
    private static final String REMOTE_SCOPE_MANIFEST = "RS";

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造通用消息序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public MiscMessageSerializer(ExtendedActorSystem system) {
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
    public String manifest(Object obj) {
        return switch (obj) {
            case Identify ignored -> IDENTIFY_MANIFEST;
            case ActorIdentity ignored -> ACTOR_IDENTITY_MANIFEST;
            case Optional<?> ignored -> OPTIONAL_MANIFEST;
            case ActorRef ignored -> ACTOR_REF_MANIFEST;
            case Status.Success ignored -> STATUS_SUCCESS_MANIFEST;
            case Status.Failure ignored -> STATUS_FAILURE_MANIFEST;
            case Throwable ignored -> THROWABLE_MANIFEST;
            case PoisonPill ignored -> POISON_PILL_MANIFEST;
            case Kill ignored -> KILL_MANIFEST;
            case RemoteWatcher.Heartbeat ignored -> REMOTE_WATCHER_HB_MANIFEST;
            case RemoteWatcher.HeartbeatRsp ignored -> REMOTE_WATCHER_HB_RESP_MANIFEST;
            case Done ignored -> DONE_MANIFEST;
            case Address ignored -> ADDRESS_MANIFEST;
            case UniqueAddress ignored -> UNIQUE_ADDRESS_MANIFEST;
            case LocalScope ignored -> LOCAL_SCOPE_MANIFEST;
            case RemoteScope ignored -> REMOTE_SCOPE_MANIFEST;
            default -> throw new IllegalArgumentException(
                    "Cannot serialize object of type [" + obj.getClass().getName() + "]");
        };
    }

    @Override
    public byte[] toBinary(Object obj) {
        try {
            return switch (obj) {
                case Identify identify -> encodePayloadField(1, identify.messageId());
                case ActorIdentity identity -> serializeActorIdentity(identity);
                case Optional<?> opt -> opt.isPresent() ? encodePayloadField(1, opt.get()) : EMPTY_BYTES;
                case ActorRef ref -> WireFormats.encodeActorRefData(Serialization.serializedActorPath(ref));
                case Status.Success s -> encodePayload(s.status());
                case Status.Failure f -> encodePayload(f.cause());
                case Throwable t -> serializeThrowable(t);
                case PoisonPill ignored -> EMPTY_BYTES;
                case Kill ignored -> EMPTY_BYTES;
                case RemoteWatcher.Heartbeat ignored -> EMPTY_BYTES;
                case RemoteWatcher.HeartbeatRsp rsp -> serializeHeartbeatRsp(rsp);
                case Done ignored -> EMPTY_BYTES;
                case Address addr -> serializeAddress(addr);
                case UniqueAddress ua -> serializeUniqueAddress(ua);
                case LocalScope ignored -> EMPTY_BYTES;
                case RemoteScope rs -> serializeRemoteScope(rs);
                default -> throw new IllegalArgumentException(
                        "Cannot serialize object of type [" + obj.getClass().getName() + "]");
            };
        } catch (NotSerializableException e) {
            throw new MessageSerializer.SerializationException("Failed to serialize " + obj.getClass().getName(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
        try {
            return switch (manifest) {
                case IDENTIFY_MANIFEST -> new Identify(decodePayloadField(bytes, 1));
                case ACTOR_IDENTITY_MANIFEST -> deserializeActorIdentity(bytes);
                case OPTIONAL_MANIFEST -> bytes.length == 0
                        ? Optional.empty()
                        : Optional.ofNullable(decodePayloadField(bytes, 1));
                case ACTOR_REF_MANIFEST -> deserializeActorRef(bytes);
                case STATUS_SUCCESS_MANIFEST -> new Status.Success(decodePayload(bytes));
                case STATUS_FAILURE_MANIFEST -> new Status.Failure((Throwable) decodePayload(bytes));
                case THROWABLE_MANIFEST -> deserializeThrowable(bytes);
                case POISON_PILL_MANIFEST -> PoisonPill.getInstance();
                case KILL_MANIFEST -> Kill.getInstance();
                case REMOTE_WATCHER_HB_MANIFEST -> RemoteWatcher.Heartbeat.INSTANCE;
                case REMOTE_WATCHER_HB_RESP_MANIFEST -> deserializeHeartbeatRsp(bytes);
                case DONE_MANIFEST -> Done.getInstance();
                case ADDRESS_MANIFEST -> WireFormats.readAddressData(CodedInputStream.newInstance(bytes));
                case UNIQUE_ADDRESS_MANIFEST -> deserializeUniqueAddress(bytes);
                case LOCAL_SCOPE_MANIFEST -> LocalScope.INSTANCE;
                case REMOTE_SCOPE_MANIFEST -> deserializeRemoteScope(bytes);
                default -> throw new NotSerializableException(
                        "Unimplemented deserialization of message with manifest [" + manifest + "] in ["
                                + getClass().getName() + "]");
            };
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException("Failed to deserialize [" + manifest + "]");
            nse.initCause(e);
            throw nse;
        }
    }

    private byte[] encodePayload(Object obj) throws IOException, NotSerializableException {
        WireFormats.SerializedMessage sm = MessageSerializer.serialize(system, obj);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        sm.writePayloadTo(out);
        out.flush();
        return baos.toByteArray();
    }

    private Object decodePayload(byte[] bytes) throws IOException, NotSerializableException {
        WireFormats.SerializedMessage sm = WireFormats.SerializedMessage.parsePayloadFrom(
                CodedInputStream.newInstance(bytes));
        return MessageSerializer.deserialize(system, sm);
    }

    private byte[] encodePayloadField(int fieldNumber, Object obj) throws IOException, NotSerializableException {
        byte[] payload = encodePayload(obj);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(fieldNumber, payload);
        out.flush();
        return baos.toByteArray();
    }

    private Object decodePayloadField(byte[] bytes, int fieldNumber) throws IOException, NotSerializableException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == fieldNumber) {
                int len = in.readRawVarint32();
                int limit = in.pushLimit(len);
                WireFormats.SerializedMessage sm = WireFormats.SerializedMessage.parsePayloadFrom(in);
                in.popLimit(limit);
                return MessageSerializer.deserialize(system, sm);
            }
            in.skipField(tag);
        }
        return null;
    }

    private byte[] serializeActorIdentity(ActorIdentity identity) throws IOException, NotSerializableException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodePayload(identity.correlationId()));
        if (identity.getActorRef().isPresent()) {
            out.writeByteArray(2, WireFormats.encodeActorRefData(
                    Serialization.serializedActorPath(identity.getActorRef().get())));
        }
        out.flush();
        return baos.toByteArray();
    }

    private ActorIdentity deserializeActorIdentity(byte[] bytes) throws IOException, NotSerializableException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        Object correlationId = null;
        Optional<ActorRef> ref = Optional.empty();
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    correlationId = MessageSerializer.deserialize(
                            system, WireFormats.SerializedMessage.parsePayloadFrom(in));
                    in.popLimit(limit);
                }
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    String path = WireFormats.readActorRefData(in);
                    in.popLimit(limit);
                    ref = Optional.of(system.provider().resolveActorRef(path));
                }
                default -> in.skipField(tag);
            }
        }
        return new ActorIdentity(correlationId, ref);
    }

    private ActorRef deserializeActorRef(byte[] bytes) throws IOException {
        String path = WireFormats.readActorRefData(CodedInputStream.newInstance(bytes));
        return system.provider().resolveActorRef(path);
    }

    private static byte[] serializeHeartbeatRsp(RemoteWatcher.HeartbeatRsp rsp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeUInt64(1, rsp.addressUid());
        out.flush();
        return baos.toByteArray();
    }

    private static RemoteWatcher.HeartbeatRsp deserializeHeartbeatRsp(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        int uid = 0;
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == 1) {
                uid = (int) in.readUInt64();
            } else {
                in.skipField(tag);
            }
        }
        return new RemoteWatcher.HeartbeatRsp(uid);
    }

    private static byte[] serializeAddress(Address addr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        WireFormats.writeAddressData(out, addr);
        out.flush();
        return baos.toByteArray();
    }

    private static byte[] serializeUniqueAddress(UniqueAddress ua) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, serializeAddress(ua.address()));
        out.writeUInt64(2, ua.uid());
        out.flush();
        return baos.toByteArray();
    }

    private static UniqueAddress deserializeUniqueAddress(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        Address addr = null;
        long uid = 0L;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    addr = WireFormats.readAddressData(in);
                    in.popLimit(limit);
                }
                case 2 -> uid = in.readUInt64();
                default -> in.skipField(tag);
            }
        }
        return new UniqueAddress(addr, uid);
    }

    private static byte[] serializeRemoteScope(RemoteScope rs) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, serializeAddress(rs.node()));
        out.flush();
        return baos.toByteArray();
    }

    private static RemoteScope deserializeRemoteScope(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        Address node = null;
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == 1) {
                int len = in.readRawVarint32();
                int limit = in.pushLimit(len);
                node = WireFormats.readAddressData(in);
                in.popLimit(limit);
            } else {
                in.skipField(tag);
            }
        }
        return new RemoteScope(node);
    }

    private byte[] serializeThrowable(Throwable t) throws IOException, NotSerializableException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, t.getClass().getName());
        if (t.getMessage() != null) {
            out.writeString(2, t.getMessage());
        }
        if (t.getCause() != null) {
            out.writeByteArray(3, encodePayload(t.getCause()));
        }
        for (StackTraceElement ste : t.getStackTrace()) {
            out.writeByteArray(4, encodeStackTraceElement(ste));
        }
        out.flush();
        return baos.toByteArray();
    }

    private Throwable deserializeThrowable(byte[] bytes) throws IOException, NotSerializableException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        String className = "";
        String message = null;
        Throwable cause = null;
        java.util.List<StackTraceElement> stack = new java.util.ArrayList<>();
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> className = in.readString();
                case 2 -> message = in.readString();
                case 3 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    cause = (Throwable) MessageSerializer.deserialize(
                            system, WireFormats.SerializedMessage.parsePayloadFrom(in));
                    in.popLimit(limit);
                }
                case 4 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    stack.add(decodeStackTraceElement(in));
                    in.popLimit(limit);
                }
                default -> in.skipField(tag);
            }
        }
        Throwable result = instantiateThrowable(className, message, cause);
        result.setStackTrace(stack.toArray(new StackTraceElement[0]));
        return result;
    }

    private Throwable instantiateThrowable(String className, String message, Throwable cause) {
        try {
            Class<? extends Throwable> clazz = system.dynamicAccess().getClassFor(className, Throwable.class);
            try {
                if (cause != null) {
                    return clazz.getConstructor(String.class, Throwable.class).newInstance(message, cause);
                }
            } catch (NoSuchMethodException ignored) {
            }
            try {
                if (message != null) {
                    Throwable t = clazz.getConstructor(String.class).newInstance(message);
                    if (cause != null) t.initCause(cause);
                    return t;
                }
            } catch (NoSuchMethodException ignored) {
            }
            Throwable t = clazz.getConstructor().newInstance();
            if (cause != null) t.initCause(cause);
            return t;
        } catch (Throwable ignored) {
            return ActorInitializationException.apply(null, message != null ? message : className, cause);
        }
    }

    private static byte[] encodeStackTraceElement(StackTraceElement ste) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, ste.getClassName());
        out.writeString(2, ste.getMethodName());
        out.writeString(3, ste.getFileName() == null ? "" : ste.getFileName());
        out.writeInt32(4, ste.getLineNumber());
        out.flush();
        return baos.toByteArray();
    }

    private static StackTraceElement decodeStackTraceElement(CodedInputStream in) throws IOException {
        String cls = "";
        String method = "";
        String file = null;
        int line = -1;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> cls = in.readString();
                case 2 -> method = in.readString();
                case 3 -> file = in.readString();
                case 4 -> line = in.readInt32();
                default -> in.skipField(tag);
            }
        }
        return new StackTraceElement(cls, method, file, line);
    }
}
