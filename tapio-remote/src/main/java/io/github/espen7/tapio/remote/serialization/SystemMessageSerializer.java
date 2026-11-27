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
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.dispatch.sysmsg.Create;
import io.github.espen7.tapio.dispatch.sysmsg.DeathWatchNotification;
import io.github.espen7.tapio.dispatch.sysmsg.Failed;
import io.github.espen7.tapio.dispatch.sysmsg.Recreate;
import io.github.espen7.tapio.dispatch.sysmsg.Resume;
import io.github.espen7.tapio.dispatch.sysmsg.Supervise;
import io.github.espen7.tapio.dispatch.sysmsg.Suspend;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Terminate;
import io.github.espen7.tapio.dispatch.sysmsg.Unwatch;
import io.github.espen7.tapio.dispatch.sysmsg.Watch;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.Serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * {@link SystemMessage} 的 Protobuf 序列化器。
 */
public class SystemMessageSerializer implements BaseSerializer {
    private static final int CREATE = 0;
    private static final int RECREATE = 1;
    private static final int SUSPEND = 2;
    private static final int RESUME = 3;
    private static final int TERMINATE = 4;
    private static final int SUPERVISE = 5;
    private static final int WATCH = 6;
    private static final int UNWATCH = 7;
    private static final int FAILED = 8;
    private static final int DEATHWATCH_NOTIFICATION = 9;

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造系统消息序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public SystemMessageSerializer(ExtendedActorSystem system) {
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
    public boolean includeManifest() {
        return false;
    }

    @Override
    public byte[] toBinary(Object o) {
        if (!(o instanceof SystemMessage sysMsg)) {
            throw new IllegalArgumentException("Cannot serialize object of type ["
                    + (o == null ? "null" : o.getClass().getName()) + "]");
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            switch (sysMsg) {
                case Create c -> {
                    out.writeEnum(1, CREATE);
                    if (c.failure.isPresent()) {
                        writePayload(out, 3, c.failure.get());
                    }
                }
                case Recreate r -> {
                    out.writeEnum(1, RECREATE);
                    writePayload(out, 3, r.cause);
                }
                case Suspend ignored -> out.writeEnum(1, SUSPEND);
                case Resume r -> {
                    out.writeEnum(1, RESUME);
                    writePayload(out, 3, r.causedByFailure);
                }
                case Terminate ignored -> out.writeEnum(1, TERMINATE);
                case Supervise s -> {
                    out.writeEnum(1, SUPERVISE);
                    out.writeByteArray(5, encodeSuperviseData(s.child, s.async));
                }
                case Watch w -> {
                    out.writeEnum(1, WATCH);
                    out.writeByteArray(2, encodeWatchData(w.watchee, w.watcher));
                }
                case Unwatch u -> {
                    out.writeEnum(1, UNWATCH);
                    out.writeByteArray(2, encodeWatchData(u.watchee, u.watcher));
                }
                case Failed f -> {
                    out.writeEnum(1, FAILED);
                    out.writeByteArray(6, encodeFailedData(f.child, f.uid));
                    writePayload(out, 3, f.cause);
                }
                case DeathWatchNotification dw -> {
                    out.writeEnum(1, DEATHWATCH_NOTIFICATION);
                    out.writeByteArray(7, encodeDeathWatchData(
                            dw.actor, dw.existenceConfirmed, dw.addressTerminated));
                }
                default -> throw new IllegalArgumentException("Unsupported SystemMessage: " + sysMsg.getClass().getName());
            }
            out.flush();
            return baos.toByteArray();
        } catch (NotSerializableException e) {
            throw new MessageSerializer.SerializationException("Failed to serialize SystemMessage", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        try {
            CodedInputStream in = CodedInputStream.newInstance(bytes);
            int type = -1;
            InternalActorRef watchWatchee = null;
            InternalActorRef watchWatcher = null;
            Object cause = null;
            ActorRef superviseChild = null;
            boolean superviseAsync = false;
            ActorRef failedChild = null;
            long failedUid = 0L;
            ActorRef dwActor = null;
            boolean dwExistenceConfirmed = false;
            boolean dwAddressTerminated = false;

            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> type = in.readEnum();
                    case 2 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        String watcheePath = "";
                        String watcherPath = "";
                        int wTag;
                        while ((wTag = in.readTag()) != 0) {
                            switch (WireFormat.getTagFieldNumber(wTag)) {
                                case 1 -> watcheePath = readActorRefPath(in);
                                case 2 -> watcherPath = readActorRefPath(in);
                                default -> in.skipField(wTag);
                            }
                        }
                        in.popLimit(limit);
                        watchWatchee = (InternalActorRef) resolveActorRef(watcheePath);
                        watchWatcher = (InternalActorRef) resolveActorRef(watcherPath);
                    }
                    case 3 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        WireFormats.SerializedMessage payload = WireFormats.SerializedMessage.parsePayloadFrom(in);
                        in.popLimit(limit);
                        cause = MessageSerializer.deserialize(system, payload);
                    }
                    case 5 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        String childPath = "";
                        int sTag;
                        while ((sTag = in.readTag()) != 0) {
                            switch (WireFormat.getTagFieldNumber(sTag)) {
                                case 1 -> childPath = readActorRefPath(in);
                                case 2 -> superviseAsync = in.readBool();
                                default -> in.skipField(sTag);
                            }
                        }
                        in.popLimit(limit);
                        superviseChild = resolveActorRef(childPath);
                    }
                    case 6 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        String childPath = "";
                        int fTag;
                        while ((fTag = in.readTag()) != 0) {
                            switch (WireFormat.getTagFieldNumber(fTag)) {
                                case 1 -> childPath = readActorRefPath(in);
                                case 2 -> failedUid = in.readUInt64();
                                default -> in.skipField(fTag);
                            }
                        }
                        in.popLimit(limit);
                        failedChild = resolveActorRef(childPath);
                    }
                    case 7 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        String actorPath = "";
                        int dTag;
                        while ((dTag = in.readTag()) != 0) {
                            switch (WireFormat.getTagFieldNumber(dTag)) {
                                case 1 -> actorPath = readActorRefPath(in);
                                case 2 -> dwExistenceConfirmed = in.readBool();
                                case 3 -> dwAddressTerminated = in.readBool();
                                default -> in.skipField(dTag);
                            }
                        }
                        in.popLimit(limit);
                        dwActor = resolveActorRef(actorPath);
                    }
                    default -> in.skipField(tag);
                }
            }
            return switch (type) {
                case CREATE -> new Create(Optional.ofNullable((io.github.espen7.tapio.actor.ActorInitializationException) cause));
                case RECREATE -> new Recreate((Throwable) cause);
                case SUSPEND -> new Suspend();
                case RESUME -> new Resume((Throwable) cause);
                case TERMINATE -> new Terminate();
                case SUPERVISE -> new Supervise(superviseChild, superviseAsync);
                case WATCH -> new Watch(watchWatchee, watchWatcher);
                case UNWATCH -> new Unwatch(watchWatchee, watchWatcher);
                case FAILED -> new Failed(failedChild, (Throwable) cause, (int) failedUid);
                case DEATHWATCH_NOTIFICATION ->
                        new DeathWatchNotification(dwActor, dwExistenceConfirmed, dwAddressTerminated);
                default -> throw new NotSerializableException("Unknown SystemMessage type: " + type);
            };
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException("Failed to deserialize SystemMessage");
            nse.initCause(e);
            throw nse;
        }
    }

    private void writePayload(CodedOutputStream out, int fieldNumber, Object obj)
            throws IOException, NotSerializableException {
        WireFormats.SerializedMessage sm = MessageSerializer.serialize(system, obj);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream sub = CodedOutputStream.newInstance(baos);
        sm.writePayloadTo(sub);
        sub.flush();
        out.writeByteArray(fieldNumber, baos.toByteArray());
    }

    private ActorRef resolveActorRef(String path) {
        return system.provider().resolveActorRef(path);
    }

    private static String readActorRefPath(CodedInputStream in) throws IOException {
        int len = in.readRawVarint32();
        int limit = in.pushLimit(len);
        String path = WireFormats.readActorRefData(in);
        in.popLimit(limit);
        return path;
    }

    private static byte[] encodeWatchData(ActorRef watchee, ActorRef watcher) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, WireFormats.encodeActorRefData(Serialization.serializedActorPath(watchee)));
        out.writeByteArray(2, WireFormats.encodeActorRefData(Serialization.serializedActorPath(watcher)));
        out.flush();
        return baos.toByteArray();
    }

    private static byte[] encodeSuperviseData(ActorRef child, boolean async) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, WireFormats.encodeActorRefData(Serialization.serializedActorPath(child)));
        out.writeBool(2, async);
        out.flush();
        return baos.toByteArray();
    }

    private static byte[] encodeFailedData(ActorRef child, long uid) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, WireFormats.encodeActorRefData(Serialization.serializedActorPath(child)));
        out.writeUInt64(2, uid);
        out.flush();
        return baos.toByteArray();
    }

    private static byte[] encodeDeathWatchData(ActorRef actor, boolean existenceConfirmed, boolean addressTerminated)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, WireFormats.encodeActorRefData(Serialization.serializedActorPath(actor)));
        out.writeBool(2, existenceConfirmed);
        out.writeBool(3, addressTerminated);
        out.flush();
        return baos.toByteArray();
    }
}
