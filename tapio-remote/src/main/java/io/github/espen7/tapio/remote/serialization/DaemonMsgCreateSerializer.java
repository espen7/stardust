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
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Scope;
import io.github.espen7.tapio.remote.DaemonMsgCreate;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.Serialization;
import io.github.espen7.tapio.serialization.SerializationExtension;
import io.github.espen7.tapio.serialization.Serializer;
import io.github.espen7.tapio.serialization.Serializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;

/**
 * {@link DaemonMsgCreate} 远程创建消息的 Protobuf 序列化器。
 */
public class DaemonMsgCreateSerializer implements BaseSerializer {
    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造守护创建消息序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public DaemonMsgCreateSerializer(ExtendedActorSystem system) {
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
        if (!(o instanceof DaemonMsgCreate msg)) {
            throw new IllegalArgumentException("Can't serialize a non-DaemonMsgCreate message ["
                    + (o == null ? "null" : o) + "]");
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            out.writeByteArray(1, serializeProps(msg.props()));
            out.writeByteArray(2, serializeDeploy(msg.deploy()));
            out.writeString(3, msg.path());
            out.writeByteArray(4, WireFormats.encodeActorRefData(Serialization.serializedActorPath(msg.supervisor())));
            out.flush();
            return baos.toByteArray();
        } catch (NotSerializableException e) {
            throw new MessageSerializer.SerializationException("Failed to serialize DaemonMsgCreate", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        try {
            CodedInputStream in = CodedInputStream.newInstance(bytes);
            Props props = null;
            Deploy deploy = null;
            String path = "";
            ActorRef supervisor = null;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        props = deserializeProps(in);
                        in.popLimit(limit);
                    }
                    case 2 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        deploy = deserializeDeploy(in);
                        in.popLimit(limit);
                    }
                    case 3 -> path = in.readString();
                    case 4 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        String supPath = WireFormats.readActorRefData(in);
                        in.popLimit(limit);
                        supervisor = system.provider().resolveActorRef(supPath);
                    }
                    default -> in.skipField(tag);
                }
            }
            return new DaemonMsgCreate(props, deploy, path, supervisor);
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException("Failed to deserialize DaemonMsgCreate");
            nse.initCause(e);
            throw nse;
        }
    }

    private byte[] serializeProps(Props props) throws IOException, NotSerializableException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(2, serializeDeploy(props.deploy()));
        out.writeString(3, props.actorClass().getName());
        out.flush();
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private Props deserializeProps(CodedInputStream in) throws IOException, NotSerializableException {
        Deploy deploy = Deploy.local;
        String clazzName = "";
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    deploy = deserializeDeploy(in);
                    in.popLimit(limit);
                }
                case 3 -> clazzName = in.readString();
                default -> in.skipField(tag);
            }
        }
        try {
            Class<? extends io.github.espen7.tapio.actor.Actor> actorClass =
                    (Class<? extends io.github.espen7.tapio.actor.Actor>) system.dynamicAccess()
                            .getClassFor(clazzName, io.github.espen7.tapio.actor.Actor.class);
            return Props.create(actorClass).withDeploy(deploy);
        } catch (ClassNotFoundException e) {
            NotSerializableException nse = new NotSerializableException("Cannot find Actor class [" + clazzName + "]");
            nse.initCause(e);
            throw nse;
        }
    }

    private byte[] serializeDeploy(Deploy deploy) throws IOException, NotSerializableException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, deploy.path());
        Serialization ser = SerializationExtension.INSTANCE.get(system);
        Serializer scopeSer = ser.findSerializerFor(deploy.scope());
        out.writeByteArray(4, scopeSer.toBinary(deploy.scope()));
        out.writeInt32(6, scopeSer.identifier());
        String scopeManifest = Serializers.manifestFor(scopeSer, deploy.scope());
        if (scopeManifest != null && !scopeManifest.isEmpty()) {
            out.writeString(7, scopeManifest);
        }
        if (!Deploy.NoDispatcherGiven.equals(deploy.dispatcher())) {
            out.writeString(5, deploy.dispatcher());
        }
        out.flush();
        return baos.toByteArray();
    }

    private Deploy deserializeDeploy(CodedInputStream in) throws IOException, NotSerializableException {
        String path = "";
        byte[] scopeBytes = null;
        int scopeSerId = -1;
        String scopeManifest = "";
        String dispatcher = Deploy.NoDispatcherGiven;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> path = in.readString();
                case 4 -> scopeBytes = in.readByteArray();
                case 5 -> dispatcher = in.readString();
                case 6 -> scopeSerId = in.readInt32();
                case 7 -> scopeManifest = in.readString();
                default -> in.skipField(tag);
            }
        }
        Scope scope = scopeBytes != null && scopeSerId >= 0
                ? (Scope) SerializationExtension.INSTANCE.get(system).deserialize(scopeBytes, scopeSerId, scopeManifest)
                : Deploy.local.scope();
        return new Deploy(path, scope, dispatcher, Deploy.NoMailboxGiven);
    }
}
