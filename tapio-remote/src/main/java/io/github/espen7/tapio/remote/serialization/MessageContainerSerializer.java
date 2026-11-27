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
import io.github.espen7.tapio.actor.ActorSelectionMessage;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.SelectChildName;
import io.github.espen7.tapio.actor.SelectChildPattern;
import io.github.espen7.tapio.actor.SelectParent;
import io.github.espen7.tapio.actor.SelectionPathElement;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ActorSelectionMessage} 容器消息的 Protobuf 序列化器。
 */
public class MessageContainerSerializer implements BaseSerializer {
    private static final int PATTERN_PARENT = 0;
    private static final int PATTERN_CHILD_NAME = 1;
    private static final int PATTERN_CHILD_PATTERN = 2;

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造消息容器序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public MessageContainerSerializer(ExtendedActorSystem system) {
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
        if (!(o instanceof ActorSelectionMessage sel)) {
            throw new IllegalArgumentException("Cannot serialize object of type ["
                    + (o == null ? "null" : o.getClass().getName()) + "]");
        }
        try {
            WireFormats.SerializedMessage envelope = MessageSerializer.serialize(system, sel.message());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            out.writeBytes(1, envelope.message());
            out.writeInt32(2, envelope.serializerId());
            if (envelope.messageManifest() != null && !envelope.messageManifest().isEmpty()) {
                out.writeBytes(4, envelope.messageManifest());
            }
            for (SelectionPathElement element : sel.elements()) {
                byte[] elemBytes = encodeSelectionElement(element);
                out.writeByteArray(3, elemBytes);
            }
            out.writeBool(5, sel.wildcardFanOut());
            out.flush();
            return baos.toByteArray();
        } catch (NotSerializableException e) {
            throw new MessageSerializer.SerializationException("Failed to serialize ActorSelectionMessage", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        try {
            CodedInputStream in = CodedInputStream.newInstance(bytes);
            com.google.protobuf.ByteString enclosed = com.google.protobuf.ByteString.EMPTY;
            int serializerId = 0;
            com.google.protobuf.ByteString msgManifest = com.google.protobuf.ByteString.EMPTY;
            List<SelectionPathElement> elements = new ArrayList<>();
            boolean wildcardFanOut = false;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> enclosed = in.readBytes();
                    case 2 -> serializerId = in.readInt32();
                    case 3 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        elements.add(decodeSelectionElement(in));
                        in.popLimit(limit);
                    }
                    case 4 -> msgManifest = in.readBytes();
                    case 5 -> wildcardFanOut = in.readBool();
                    default -> in.skipField(tag);
                }
            }
            Object payload = MessageSerializer.deserialize(
                    system, new WireFormats.SerializedMessage(enclosed, serializerId, msgManifest));
            return new ActorSelectionMessage(payload, List.copyOf(elements), wildcardFanOut);
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException("Failed to deserialize ActorSelectionMessage");
            nse.initCause(e);
            throw nse;
        }
    }

    private static byte[] encodeSelectionElement(SelectionPathElement element) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        switch (element) {
            case SelectParent ignored -> out.writeEnum(1, PATTERN_PARENT);
            case SelectChildName childName -> {
                out.writeEnum(1, PATTERN_CHILD_NAME);
                out.writeString(2, childName.name());
            }
            case SelectChildPattern childPattern -> {
                out.writeEnum(1, PATTERN_CHILD_PATTERN);
                out.writeString(2, childPattern.patternStr());
            }
        }
        out.flush();
        return baos.toByteArray();
    }

    private static SelectionPathElement decodeSelectionElement(CodedInputStream in) throws IOException {
        int type = -1;
        String matcher = "";
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> type = in.readEnum();
                case 2 -> matcher = in.readString();
                default -> in.skipField(tag);
            }
        }
        return switch (type) {
            case PATTERN_PARENT -> SelectParent.INSTANCE;
            case PATTERN_CHILD_NAME -> new SelectChildName(matcher);
            case PATTERN_CHILD_PATTERN -> new SelectChildPattern(matcher);
            default -> throw new IOException("Unknown Selection PatternType: " + type);
        };
    }
}
