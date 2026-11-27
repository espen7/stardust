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
package io.github.espen7.tapio.remote.transport;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.Serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * INTERNAL API：Tapio 协议数据单元（PDU）编解码器。
 */
@InternalApi
public final class TapioPduCodec {
    private TapioPduCodec() {
    }

    /**
     * 握手信息。
     *
     * @param origin 发起方地址
     * @param uid 发起方系统 UID
     */
    public record HandshakeInfo(Address origin, long uid) {
    }

    /**
     * 解码后的 Tapio PDU 密封层次。
     */
    public sealed interface TapioPdu permits Associate, Disassociate, Heartbeat, Payload {
    }

    /** 握手建立指令。 */
    public record Associate(HandshakeInfo info) implements TapioPdu {
    }

    /** 断开关联指令。 */
    public record Disassociate(AssociationHandle.DisassociateInfo reason) implements TapioPdu {
    }

    /** 链路心跳指令。 */
    public static final class Heartbeat implements TapioPdu {
        /** 单例实例。 */
        public static final Heartbeat INSTANCE = new Heartbeat();

        private Heartbeat() {
        }
    }

    /** 业务消息载荷帧。 */
    public record Payload(ByteString bytes) implements TapioPdu {
    }

    /**
     * 解码后的远程业务消息。
     *
     * @param recipient 本地接收方引用
     * @param recipientAddress 接收方地址
     * @param serializedMessage 序列化后的消息载荷
     * @param senderOption 可选发送方引用
     * @param seq 可选可靠投递序号
     */
    public record DecodedMessage(
            InternalActorRef recipient,
            Address recipientAddress,
            WireFormats.SerializedMessage serializedMessage,
            Optional<ActorRef> senderOption,
            Optional<Long> seq) {
    }

    /**
     * 解码后的容器载荷（可能同时携带 ACK 与业务消息）。
     *
     * @param ack 可选确认信息
     * @param message 可选业务消息
     */
    public record DecodedContainer(Optional<WireFormats.AcknowledgementInfo> ack, Optional<DecodedMessage> message) {
    }

    /**
     * 解码原始 PDU 字节串。
     *
     * @param raw 原始字节串
     * @return 解码后的 {@link TapioPdu}
     */
    public static TapioPdu decodePdu(ByteString raw) {
        try {
            CodedInputStream in = raw.newCodedInput();
            ByteString payload = null;
            WireFormats.CommandType cmdType = null;
            HandshakeInfo handshakeInfo = null;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> payload = in.readBytes();
                    case 2 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        int cTag;
                        while ((cTag = in.readTag()) != 0) {
                            switch (WireFormat.getTagFieldNumber(cTag)) {
                                case 1 -> cmdType = WireFormats.CommandType.forNumber(in.readEnum());
                                case 2 -> {
                                    int hLen = in.readRawVarint32();
                                    int hLimit = in.pushLimit(hLen);
                                    handshakeInfo = readHandshakeInfo(in);
                                    in.popLimit(hLimit);
                                }
                                default -> in.skipField(cTag);
                            }
                        }
                        in.popLimit(limit);
                    }
                    default -> in.skipField(tag);
                }
            }
            if (payload != null) {
                return new Payload(payload);
            }
            if (cmdType != null) {
                return switch (cmdType) {
                    case ASSOCIATE -> new Associate(handshakeInfo);
                    case DISASSOCIATE -> new Disassociate(AssociationHandle.Unknown.INSTANCE);
                    case DISASSOCIATE_SHUTTING_DOWN -> new Disassociate(AssociationHandle.Shutdown.INSTANCE);
                    case DISASSOCIATE_QUARANTINED -> new Disassociate(AssociationHandle.Quarantined.INSTANCE);
                    case HEARTBEAT -> Heartbeat.INSTANCE;
                };
            }
            throw new IllegalArgumentException("Invalid PDU: neither payload nor instruction present");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 构造握手 PDU。
     *
     * @param info 握手信息
     * @return 编码后的 PDU
     */
    public static ByteString constructAssociate(HandshakeInfo info) {
        return encodeControlMessage(WireFormats.CommandType.ASSOCIATE, info);
    }

    /**
     * 构造断开关联 PDU。
     *
     * @param reason 断开原因
     * @return 编码后的 PDU
     */
    public static ByteString constructDisassociate(AssociationHandle.DisassociateInfo reason) {
        WireFormats.CommandType cmd = switch (reason) {
            case AssociationHandle.Shutdown ignored -> WireFormats.CommandType.DISASSOCIATE_SHUTTING_DOWN;
            case AssociationHandle.Quarantined ignored -> WireFormats.CommandType.DISASSOCIATE_QUARANTINED;
            case AssociationHandle.Unknown ignored -> WireFormats.CommandType.DISASSOCIATE;
        };
        return encodeControlMessage(cmd, null);
    }

    /**
     * 构造心跳 PDU。
     *
     * @return 编码后的 PDU
     */
    public static ByteString constructHeartbeat() {
        return encodeControlMessage(WireFormats.CommandType.HEARTBEAT, null);
    }

    /**
     * 构造载荷 PDU。
     *
     * @param payload 容器字节串
     * @return 编码后的 PDU
     */
    public static ByteString constructPayload(ByteString payload) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            out.writeBytes(1, payload);
            out.flush();
            return ByteString.copyFrom(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 构造 {@code AckAndEnvelopeContainer} 字节串。
     *
     * @param localAddress 本地地址
     * @param recipient 接收方引用
     * @param serializedMessage 序列化载荷
     * @param senderOption 发送方引用（{@link ActorRef#noSender()} 表示无发送方）
     * @param seq 可选序号
     * @param ackOption 可选确认信息
     * @return 编码后的容器字节串
     */
    public static ByteString constructMessage(
            Address localAddress,
            ActorRef recipient,
            WireFormats.SerializedMessage serializedMessage,
            Optional<ActorRef> senderOption,
            Optional<Long> seq,
            Optional<WireFormats.AcknowledgementInfo> ackOption) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            if (ackOption.isPresent()) {
                ByteArrayOutputStream ackBaos = new ByteArrayOutputStream();
                CodedOutputStream ackOut = CodedOutputStream.newInstance(ackBaos);
                ackOption.get().writeTo(ackOut);
                ackOut.flush();
                out.writeByteArray(1, ackBaos.toByteArray());
            }
            String recipientPath = recipient.path().toSerializationFormat();
            String senderPath = null;
            if (senderOption.isPresent() && senderOption.get() != ActorRef.noSender) {
                senderPath = Serialization.serializedActorPath(senderOption.get());
            }
            WireFormats.RemoteEnvelope env = new WireFormats.RemoteEnvelope(
                    recipientPath, serializedMessage, senderPath, seq.orElse(null));
            ByteArrayOutputStream envBaos = new ByteArrayOutputStream();
            CodedOutputStream envOut = CodedOutputStream.newInstance(envBaos);
            env.writeTo(envOut);
            envOut.flush();
            out.writeByteArray(2, envBaos.toByteArray());
            out.flush();
            return ByteString.copyFrom(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 构造仅包含确认信息的 {@code AckAndEnvelopeContainer} 字节串。
     *
     * @param ack 确认信息
     * @return 编码后的容器字节串
     */
    public static ByteString constructPureAck(WireFormats.AcknowledgementInfo ack) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            ByteArrayOutputStream ackBaos = new ByteArrayOutputStream();
            CodedOutputStream ackOut = CodedOutputStream.newInstance(ackBaos);
            ack.writeTo(ackOut);
            ackOut.flush();
            out.writeByteArray(1, ackBaos.toByteArray());
            out.flush();
            return ByteString.copyFrom(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 解码 {@code AckAndEnvelopeContainer} 字节串。
     *
     * @param raw 容器字节串
     * @param provider 用于解析 {@link ActorRef} 的 Provider
     * @param localAddress 本地地址
     * @return 解码结果
     */
    public static DecodedContainer decodeMessage(ByteString raw, ActorRefProvider provider, Address localAddress) {
        try {
            CodedInputStream in = raw.newCodedInput();
            Optional<WireFormats.AcknowledgementInfo> ack = Optional.empty();
            WireFormats.RemoteEnvelope envelope = null;
            int tag;
            while ((tag = in.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        ack = Optional.of(WireFormats.AcknowledgementInfo.parseFrom(in));
                        in.popLimit(limit);
                    }
                    case 2 -> {
                        int len = in.readRawVarint32();
                        int limit = in.pushLimit(len);
                        envelope = WireFormats.RemoteEnvelope.parseFrom(in);
                        in.popLimit(limit);
                    }
                    default -> in.skipField(tag);
                }
            }
            if (envelope == null) {
                return new DecodedContainer(ack, Optional.empty());
            }
            InternalActorRef recipient = (InternalActorRef) provider.resolveActorRef(envelope.recipientPath());
            Optional<ActorRef> sender = envelope.senderPath() == null
                    ? Optional.empty()
                    : Optional.of(provider.resolveActorRef(envelope.senderPath()));
            DecodedMessage msg = new DecodedMessage(
                    recipient,
                    recipient.path().address(),
                    envelope.message(),
                    sender,
                    Optional.ofNullable(envelope.seq()));
            return new DecodedContainer(ack, Optional.of(msg));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ByteString encodeControlMessage(WireFormats.CommandType type, HandshakeInfo info) {
        try {
            ByteArrayOutputStream ctrlBaos = new ByteArrayOutputStream();
            CodedOutputStream ctrlOut = CodedOutputStream.newInstance(ctrlBaos);
            ctrlOut.writeEnum(1, type.number());
            if (info != null) {
                ByteArrayOutputStream hsBaos = new ByteArrayOutputStream();
                CodedOutputStream hsOut = CodedOutputStream.newInstance(hsBaos);
                ByteArrayOutputStream addrBaos = new ByteArrayOutputStream();
                CodedOutputStream addrOut = CodedOutputStream.newInstance(addrBaos);
                WireFormats.writeAddressData(addrOut, info.origin());
                addrOut.flush();
                hsOut.writeByteArray(1, addrBaos.toByteArray());
                hsOut.writeFixed64(2, info.uid());
                hsOut.flush();
                ctrlOut.writeByteArray(2, hsBaos.toByteArray());
            }
            ctrlOut.flush();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(baos);
            out.writeByteArray(2, ctrlBaos.toByteArray());
            out.flush();
            return ByteString.copyFrom(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static HandshakeInfo readHandshakeInfo(CodedInputStream in) throws IOException {
        Address origin = null;
        long uid = 0L;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    origin = WireFormats.readAddressData(in);
                    in.popLimit(limit);
                }
                case 2 -> uid = in.readFixed64();
                default -> in.skipField(tag);
            }
        }
        return new HandshakeInfo(origin, uid);
    }
}
