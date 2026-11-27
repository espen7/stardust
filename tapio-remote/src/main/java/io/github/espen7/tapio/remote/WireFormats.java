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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.annotation.InternalApi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * INTERNAL API：Classic Remoting Protobuf 线协议数据结构与编解码器。
 */
@InternalApi
public final class WireFormats {
    private WireFormats() {
    }

    /**
     * 序列化后的消息载荷信封，对应 {@code WireFormats.SerializedMessage} 与 {@code ContainerFormats.Payload}。
     *
     * @param message 序列化后的字节
     * @param serializerId 序列化器 ID
     * @param messageManifest 可选 manifest 字节（UTF-8）
     */
    public record SerializedMessage(ByteString message, int serializerId, ByteString messageManifest) {
        public byte[] toByteArray() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CodedOutputStream out = CodedOutputStream.newInstance(baos);
                writeTo(out);
                out.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeBytes(1, message);
            out.writeInt32(2, serializerId);
            if (messageManifest != null && !messageManifest.isEmpty()) {
                out.writeBytes(3, messageManifest);
            }
        }

        public void writePayloadTo(CodedOutputStream out) throws IOException {
            out.writeBytes(1, message);
            out.writeInt32(2, serializerId);
            if (messageManifest != null && !messageManifest.isEmpty()) {
                out.writeBytes(4, messageManifest);
            }
        }

        public static SerializedMessage parseFrom(CodedInputStream in) throws IOException {
            ByteString msg = ByteString.EMPTY;
            int serId = 0;
            ByteString manifest = ByteString.EMPTY;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> msg = in.readBytes();
                    case 2 -> serId = in.readInt32();
                    case 3 -> manifest = in.readBytes();
                    default -> in.skipField(tag);
                }
            }
            return new SerializedMessage(msg, serId, manifest);
        }

        public static SerializedMessage parsePayloadFrom(CodedInputStream in) throws IOException {
            ByteString msg = ByteString.EMPTY;
            int serId = 0;
            ByteString manifest = ByteString.EMPTY;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> msg = in.readBytes();
                    case 2 -> serId = in.readInt32();
                    case 4 -> manifest = in.readBytes();
                    default -> in.skipField(tag);
                }
            }
            return new SerializedMessage(msg, serId, manifest);
        }
    }

    /**
     * 远程节点地址线协议表示，对应 {@code WireFormats.AddressData}。
     */
    public static void writeAddressData(CodedOutputStream out, Address addr) throws IOException {
        out.writeString(1, addr.system());
        out.writeString(2, addr.host() == null ? "" : addr.host());
        out.writeUInt32(3, addr.port() == null ? 0 : addr.port());
        out.writeString(4, addr.protocol());
    }

    /**
     * 从 {@link CodedInputStream} 读取 {@code WireFormats.AddressData}。
     *
     * @param in 输入流
     * @return 解析得到的 {@link Address}
     * @throws IOException 读取失败时抛出
     */
    public static Address readAddressData(CodedInputStream in) throws IOException {
        String system = "";
        String hostname = "";
        int port = 0;
        String protocol = "tapio";
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> system = in.readString();
                case 2 -> hostname = in.readString();
                case 3 -> port = in.readUInt32();
                case 4 -> protocol = in.readString();
                default -> in.skipField(tag);
            }
        }
        return new Address(protocol, system, hostname, port);
    }

    /**
     * 确认与重传信息，对应 {@code WireFormats.AcknowledgementInfo}。
     *
     * @param cumulativeAck 累积确认序号
     * @param nacks 否定确认序号列表
     */
    public record AcknowledgementInfo(long cumulativeAck, List<Long> nacks) {
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeFixed64(1, cumulativeAck);
            for (long nack : nacks) {
                out.writeFixed64(2, nack);
            }
        }

        public static AcknowledgementInfo parseFrom(CodedInputStream in) throws IOException {
            long ack = 0L;
            List<Long> nacks = new ArrayList<>();
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> ack = in.readFixed64();
                    case 2 -> nacks.add(in.readFixed64());
                    default -> in.skipField(tag);
                }
            }
            return new AcknowledgementInfo(ack, List.copyOf(nacks));
        }
    }

    /**
     * 远程消息信封，对应 {@code WireFormats.RemoteEnvelope}。
     *
     * @param recipientPath 接收方路径
     * @param message 序列化载荷
     * @param senderPath 可选发送方路径
     * @param seq 可选可靠投递序号
     */
    public record RemoteEnvelope(String recipientPath, SerializedMessage message, String senderPath, Long seq) {
        public void writeTo(CodedOutputStream out) throws IOException {
            byte[] recBytes = encodeActorRefData(recipientPath);
            out.writeByteArray(1, recBytes);
            byte[] msgBytes = message.toByteArray();
            out.writeByteArray(2, msgBytes);
            if (senderPath != null) {
                out.writeByteArray(4, encodeActorRefData(senderPath));
            }
            if (seq != null) {
                out.writeFixed64(5, seq);
            }
        }

        public static RemoteEnvelope parseFrom(CodedInputStream in) throws IOException {
            String recipientPath = null;
            SerializedMessage message = null;
            String senderPath = null;
            Long seq = null;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        recipientPath = readActorRefData(in);
                        in.popLimit(limit);
                    }
                    case 2 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        message = SerializedMessage.parseFrom(in);
                        in.popLimit(limit);
                    }
                    case 4 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        senderPath = readActorRefData(in);
                        in.popLimit(limit);
                    }
                    case 5 -> seq = in.readFixed64();
                    default -> in.skipField(tag);
                }
            }
            return new RemoteEnvelope(recipientPath, message, senderPath, seq);
        }
    }

    /**
     * 编码 {@code ActorRefData}（仅含字段 1：path）。
     *
     * @param path ActorRef 序列化路径
     * @return 编码字节
     * @throws IOException 编码失败时抛出
     */
    public static byte[] encodeActorRefData(String path) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, path);
        out.flush();
        return baos.toByteArray();
    }

    /**
     * 从 {@link CodedInputStream} 读取 {@code ActorRefData}。
     *
     * @param in 输入流
     * @return ActorRef 路径
     * @throws IOException 读取失败时抛出
     */
    public static String readActorRefData(CodedInputStream in) throws IOException {
        String path = "";
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == 1) {
                path = in.readString();
            } else {
                in.skipField(tag);
            }
        }
        return path;
    }

    /**
     * 控制指令类型，对应 {@code WireFormats.CommandType}。
     */
    public enum CommandType {
        ASSOCIATE(1),
        DISASSOCIATE(2),
        HEARTBEAT(3),
        DISASSOCIATE_SHUTTING_DOWN(4),
        DISASSOCIATE_QUARANTINED(5);

        private final int number;

        CommandType(int number) {
            this.number = number;
        }

        public int number() {
            return number;
        }

        public static CommandType forNumber(int number) {
            return switch (number) {
                case 1 -> ASSOCIATE;
                case 2 -> DISASSOCIATE;
                case 3 -> HEARTBEAT;
                case 4 -> DISASSOCIATE_SHUTTING_DOWN;
                case 5 -> DISASSOCIATE_QUARANTINED;
                default -> null;
            };
        }
    }
}
