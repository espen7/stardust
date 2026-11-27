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
package io.github.espen7.tapio.cluster.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.cluster.ClusterMessages;
import io.github.espen7.tapio.cluster.Gossip;
import io.github.espen7.tapio.cluster.GossipOverview;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.cluster.Reachability;
import io.github.espen7.tapio.cluster.UniqueAddress;
import io.github.espen7.tapio.cluster.VectorClock;
import io.github.espen7.tapio.remote.MessageSerializer;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.SerializerWithStringManifest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 集群内部消息与 Gossip 状态的 Protobuf 序列化器。
 */
public class ClusterMessageSerializer extends SerializerWithStringManifest implements BaseSerializer {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final String JOIN_MANIFEST = "J";
    private static final String WELCOME_MANIFEST = "W";
    private static final String LEAVE_MANIFEST = "L";
    private static final String DOWN_MANIFEST = "D";
    private static final String INIT_JOIN_MANIFEST = "IJ";
    private static final String INIT_JOIN_ACK_MANIFEST = "IJA";
    private static final String INIT_JOIN_NACK_MANIFEST = "IJN";
    private static final String HEARTBEAT_MANIFEST = "HB";
    private static final String HEARTBEAT_RSP_MANIFEST = "HBR";
    private static final String EXITING_CONFIRMED_MANIFEST = "EC";
    private static final String GOSSIP_STATUS_MANIFEST = "GS";
    private static final String GOSSIP_ENVELOPE_MANIFEST = "GE";

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造集群消息序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public ClusterMessageSerializer(ExtendedActorSystem system) {
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
            case ClusterMessages.Join ignored -> JOIN_MANIFEST;
            case ClusterMessages.Welcome ignored -> WELCOME_MANIFEST;
            case ClusterMessages.Leave ignored -> LEAVE_MANIFEST;
            case ClusterMessages.Down ignored -> DOWN_MANIFEST;
            case ClusterMessages.InitJoin ignored -> INIT_JOIN_MANIFEST;
            case ClusterMessages.InitJoinAck ignored -> INIT_JOIN_ACK_MANIFEST;
            case ClusterMessages.InitJoinNack ignored -> INIT_JOIN_NACK_MANIFEST;
            case ClusterMessages.Heartbeat ignored -> HEARTBEAT_MANIFEST;
            case ClusterMessages.HeartbeatRsp ignored -> HEARTBEAT_RSP_MANIFEST;
            case ClusterMessages.ExitingConfirmed ignored -> EXITING_CONFIRMED_MANIFEST;
            case ClusterMessages.GossipStatus ignored -> GOSSIP_STATUS_MANIFEST;
            case ClusterMessages.GossipEnvelope ignored -> GOSSIP_ENVELOPE_MANIFEST;
            default -> throw new IllegalArgumentException(
                    "Can't serialize object of type " + obj.getClass() + " in [" + getClass().getName() + "]");
        };
    }

    @Override
    public byte[] toBinary(Object obj) {
        try {
            return switch (obj) {
                case ClusterMessages.Join j -> encodeJoin(j);
                case ClusterMessages.Welcome w -> encodeWelcome(w);
                case ClusterMessages.Leave l -> encodeAddress(l.address());
                case ClusterMessages.Down d -> encodeAddress(d.address());
                case ClusterMessages.InitJoin ignored -> EMPTY_BYTES;
                case ClusterMessages.InitJoinAck ack -> encodeInitJoinAck(ack.address());
                case ClusterMessages.InitJoinNack nack -> encodeAddress(nack.address());
                case ClusterMessages.Heartbeat hb -> encodeHeartbeat(hb);
                case ClusterMessages.HeartbeatRsp rsp -> encodeHeartbeatRsp(rsp);
                case ClusterMessages.ExitingConfirmed ec -> encodeUniqueAddress(ec.node());
                case ClusterMessages.GossipStatus gs -> encodeGossipStatus(gs);
                case ClusterMessages.GossipEnvelope ge -> encodeGossipEnvelope(ge);
                default -> throw new IllegalArgumentException(
                        "Can't serialize object of type " + obj.getClass() + " in [" + getClass().getName() + "]");
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
                case JOIN_MANIFEST -> decodeJoin(in);
                case WELCOME_MANIFEST -> decodeWelcome(in);
                case LEAVE_MANIFEST -> new ClusterMessages.Leave(WireFormats.readAddressData(in));
                case DOWN_MANIFEST -> new ClusterMessages.Down(WireFormats.readAddressData(in));
                case INIT_JOIN_MANIFEST -> ClusterMessages.InitJoin.INSTANCE;
                case INIT_JOIN_ACK_MANIFEST -> decodeInitJoinAck(in);
                case INIT_JOIN_NACK_MANIFEST -> new ClusterMessages.InitJoinNack(WireFormats.readAddressData(in));
                case HEARTBEAT_MANIFEST -> decodeHeartbeat(in);
                case HEARTBEAT_RSP_MANIFEST -> decodeHeartbeatRsp(in);
                case EXITING_CONFIRMED_MANIFEST -> new ClusterMessages.ExitingConfirmed(decodeUniqueAddress(in));
                case GOSSIP_STATUS_MANIFEST -> decodeGossipStatus(in);
                case GOSSIP_ENVELOPE_MANIFEST -> decodeGossipEnvelope(in);
                default -> throw new NotSerializableException("Unknown cluster message manifest [" + manifest + "]");
            };
        } catch (NotSerializableException e) {
            throw e;
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException("Failed to deserialize [" + manifest + "]");
            nse.initCause(e);
            throw nse;
        }
    }

    private static byte[] encodeAddress(Address addr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        WireFormats.writeAddressData(out, addr);
        out.flush();
        return baos.toByteArray();
    }

    private static byte[] encodeUniqueAddress(UniqueAddress ua) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeAddress(ua.address()));
        out.writeUInt64(2, ua.longUid());
        out.flush();
        return baos.toByteArray();
    }

    private static UniqueAddress decodeUniqueAddress(CodedInputStream in) throws IOException {
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

    private static byte[] encodeJoin(ClusterMessages.Join join) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeUniqueAddress(join.node()));
        for (String role : join.roles()) {
            out.writeString(2, role);
        }
        out.writeString(3, join.appVersion());
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.Join decodeJoin(CodedInputStream in) throws IOException {
        UniqueAddress node = null;
        Set<String> roles = new HashSet<>();
        String appVersion = "0.1.0";
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    node = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 2 -> roles.add(in.readString());
                case 3 -> appVersion = in.readString();
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.Join(node, Set.copyOf(roles), appVersion);
    }

    private static byte[] encodeInitJoinAck(Address addr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeAddress(addr));
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.InitJoinAck decodeInitJoinAck(CodedInputStream in) throws IOException {
        Address addr = null;
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == 1) {
                int len = in.readRawVarint32();
                int limit = in.pushLimit(len);
                addr = WireFormats.readAddressData(in);
                in.popLimit(limit);
            } else {
                in.skipField(tag);
            }
        }
        return new ClusterMessages.InitJoinAck(addr);
    }

    private static byte[] encodeHeartbeat(ClusterMessages.Heartbeat hb) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeAddress(hb.from()));
        out.writeInt64(2, hb.sequenceNr());
        out.writeSInt64(3, hb.creationTime());
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.Heartbeat decodeHeartbeat(CodedInputStream in) throws IOException {
        Address from = null;
        long seq = 0L;
        long ct = 0L;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    from = WireFormats.readAddressData(in);
                    in.popLimit(limit);
                }
                case 2 -> seq = in.readInt64();
                case 3 -> ct = in.readSInt64();
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.Heartbeat(from, seq, ct);
    }

    private static byte[] encodeHeartbeatRsp(ClusterMessages.HeartbeatRsp rsp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeUniqueAddress(rsp.from()));
        out.writeInt64(2, rsp.sequenceNr());
        out.writeInt64(3, rsp.creationTime());
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.HeartbeatRsp decodeHeartbeatRsp(CodedInputStream in) throws IOException {
        UniqueAddress from = null;
        long seq = 0L;
        long ct = 0L;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    from = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 2 -> seq = in.readInt64();
                case 3 -> ct = in.readInt64();
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.HeartbeatRsp(from, seq, ct);
    }

    private static byte[] encodeWelcome(ClusterMessages.Welcome w) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeUniqueAddress(w.from()));
        out.writeByteArray(2, encodeGossip(w.gossip()));
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.Welcome decodeWelcome(CodedInputStream in) throws IOException {
        UniqueAddress from = null;
        Gossip gossip = Gossip.EMPTY;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    from = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    gossip = decodeGossip(in);
                    in.popLimit(limit);
                }
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.Welcome(from, gossip);
    }

    private static byte[] encodeGossipStatus(ClusterMessages.GossipStatus gs) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeUniqueAddress(gs.from()));
        for (UniqueAddress ua : gs.seen()) {
            out.writeByteArray(2, encodeUniqueAddress(ua));
        }
        out.writeByteArray(3, encodeVectorClock(gs.version()));
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.GossipStatus decodeGossipStatus(CodedInputStream in) throws IOException {
        UniqueAddress from = null;
        Set<UniqueAddress> seen = new HashSet<>();
        VectorClock vc = VectorClock.EMPTY;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    from = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    seen.add(decodeUniqueAddress(in));
                    in.popLimit(limit);
                }
                case 3 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    vc = decodeVectorClock(in);
                    in.popLimit(limit);
                }
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.GossipStatus(from, vc, Set.copyOf(seen));
    }

    private static byte[] encodeGossipEnvelope(ClusterMessages.GossipEnvelope ge) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeByteArray(1, encodeUniqueAddress(ge.from()));
        out.writeByteArray(2, encodeUniqueAddress(ge.to()));
        out.writeByteArray(3, encodeGossip(ge.gossip()));
        out.flush();
        return baos.toByteArray();
    }

    private static ClusterMessages.GossipEnvelope decodeGossipEnvelope(CodedInputStream in) throws IOException {
        UniqueAddress from = null;
        UniqueAddress to = null;
        Gossip gossip = Gossip.EMPTY;
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    from = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    to = decodeUniqueAddress(in);
                    in.popLimit(limit);
                }
                case 3 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    gossip = decodeGossip(in);
                    in.popLimit(limit);
                }
                default -> in.skipField(tag);
            }
        }
        return new ClusterMessages.GossipEnvelope(from, to, gossip);
    }

    private static byte[] encodeGossip(Gossip g) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        List<UniqueAddress> allAddresses = new ArrayList<>();
        Map<UniqueAddress, Integer> addrIndex = new HashMap<>();
        for (Member m : g.members()) {
            addAddrIndex(m.uniqueAddress(), allAddresses, addrIndex);
        }
        for (UniqueAddress ua : g.overview().seen()) {
            addAddrIndex(ua, allAddresses, addrIndex);
        }
        for (Reachability.Record r : g.overview().reachability().records()) {
            addAddrIndex(r.observer(), allAddresses, addrIndex);
            addAddrIndex(r.subject(), allAddresses, addrIndex);
        }
        for (UniqueAddress ua : g.overview().reachability().versions().keySet()) {
            addAddrIndex(ua, allAddresses, addrIndex);
        }
        for (UniqueAddress ua : g.tombstones().keySet()) {
            addAddrIndex(ua, allAddresses, addrIndex);
        }

        List<String> allRoles = new ArrayList<>();
        Map<String, Integer> roleIndex = new HashMap<>();
        for (Member m : g.members()) {
            for (String role : m.roles()) {
                if (!roleIndex.containsKey(role)) {
                    roleIndex.put(role, allRoles.size());
                    allRoles.add(role);
                }
            }
        }

        for (UniqueAddress ua : allAddresses) {
            out.writeByteArray(1, encodeUniqueAddress(ua));
        }
        for (String role : allRoles) {
            out.writeString(2, role);
        }
        for (Member m : g.members()) {
            out.writeByteArray(3, encodeMember(m, addrIndex, roleIndex));
        }
        out.writeByteArray(4, encodeOverview(g.overview(), addrIndex));
        out.writeByteArray(5, encodeVectorClock(g.version()));
        for (Map.Entry<UniqueAddress, Long> entry : g.tombstones().entrySet()) {
            ByteArrayOutputStream tBaos = new ByteArrayOutputStream();
            CodedOutputStream tOut = CodedOutputStream.newInstance(tBaos);
            tOut.writeInt32(1, addrIndex.get(entry.getKey()));
            tOut.writeInt64(2, entry.getValue());
            tOut.flush();
            out.writeByteArray(6, tBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private static Gossip decodeGossip(CodedInputStream in) throws IOException {
        List<UniqueAddress> allAddresses = new ArrayList<>();
        List<String> allRoles = new ArrayList<>();
        List<Member> membersList = new ArrayList<>();
        GossipOverview overview = GossipOverview.EMPTY;
        VectorClock version = VectorClock.EMPTY;
        Map<UniqueAddress, Long> tombstones = new HashMap<>();
        List<int[]> rawTombstones = new ArrayList<>();
        List<Long> rawTombstoneTimes = new ArrayList<>();
        byte[] overviewBytes = null;
        List<byte[]> rawMembers = new ArrayList<>();

        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    allAddresses.add(decodeUniqueAddress(in));
                    in.popLimit(limit);
                }
                case 2 -> allRoles.add(in.readString());
                case 3 -> rawMembers.add(in.readByteArray());
                case 4 -> overviewBytes = in.readByteArray();
                case 5 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    version = decodeVectorClock(in);
                    in.popLimit(limit);
                }
                case 6 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    int idx = 0;
                    long ts = 0L;
                    int tTag;
                    while ((tTag = in.readTag()) != 0) {
                        switch (WireFormat.getTagFieldNumber(tTag)) {
                            case 1 -> idx = in.readInt32();
                            case 2 -> ts = in.readInt64();
                            default -> in.skipField(tTag);
                        }
                    }
                    in.popLimit(limit);
                    rawTombstones.add(new int[] { idx });
                    rawTombstoneTimes.add(ts);
                }
                default -> in.skipField(tag);
            }
        }

        for (byte[] rm : rawMembers) {
            membersList.add(decodeMember(CodedInputStream.newInstance(rm), allAddresses, allRoles));
        }
        if (overviewBytes != null) {
            overview = decodeOverview(CodedInputStream.newInstance(overviewBytes), allAddresses);
        }
        for (int i = 0; i < rawTombstones.size(); i++) {
            tombstones.put(allAddresses.get(rawTombstones.get(i)[0]), rawTombstoneTimes.get(i));
        }
        NavigableSet<Member> memberSet = new TreeSet<>(Member.ORDERING);
        memberSet.addAll(membersList);
        return new Gossip(memberSet, overview, version, tombstones);
    }

    private static void addAddrIndex(UniqueAddress ua, List<UniqueAddress> list, Map<UniqueAddress, Integer> map) {
        if (!map.containsKey(ua)) {
            map.put(ua, list.size());
            list.add(ua);
        }
    }

    private static byte[] encodeMember(
            Member m, Map<UniqueAddress, Integer> addrIndex, Map<String, Integer> roleIndex) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeInt32(1, addrIndex.get(m.uniqueAddress()));
        out.writeInt32(2, m.upNumber());
        out.writeInt32(3, m.status().ordinal());
        for (String role : m.roles()) {
            out.writeInt32(4, roleIndex.get(role));
        }
        out.writeString(5, m.appVersion());
        out.flush();
        return baos.toByteArray();
    }

    private static Member decodeMember(
            CodedInputStream in, List<UniqueAddress> addrs, List<String> allRoles) throws IOException {
        int addrIdx = 0;
        int upNumber = Integer.MAX_VALUE;
        MemberStatus status = MemberStatus.Joining;
        Set<String> roles = new HashSet<>();
        String appVersion = "0.1.0";
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> addrIdx = in.readInt32();
                case 2 -> upNumber = in.readInt32();
                case 3 -> status = MemberStatus.values()[in.readInt32()];
                case 4 -> roles.add(allRoles.get(in.readInt32()));
                case 5 -> appVersion = in.readString();
                default -> in.skipField(tag);
            }
        }
        return new Member(addrs.get(addrIdx), upNumber, status, roles, appVersion);
    }

    private static byte[] encodeOverview(
            GossipOverview overview, Map<UniqueAddress, Integer> addrIndex) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (UniqueAddress ua : overview.seen()) {
            out.writeInt32(1, addrIndex.get(ua));
        }
        for (Reachability.Record r : overview.reachability().records()) {
            ByteArrayOutputStream rBaos = new ByteArrayOutputStream();
            CodedOutputStream rOut = CodedOutputStream.newInstance(rBaos);
            rOut.writeInt32(1, addrIndex.get(r.observer()));
            rOut.writeInt32(2, addrIndex.get(r.subject()));
            rOut.writeInt32(3, r.status().ordinal());
            rOut.writeInt64(4, r.version());
            rOut.flush();
            out.writeByteArray(2, rBaos.toByteArray());
        }
        for (Map.Entry<UniqueAddress, Long> e : overview.reachability().versions().entrySet()) {
            ByteArrayOutputStream vBaos = new ByteArrayOutputStream();
            CodedOutputStream vOut = CodedOutputStream.newInstance(vBaos);
            vOut.writeInt32(1, addrIndex.get(e.getKey()));
            vOut.writeInt64(2, e.getValue());
            vOut.flush();
            out.writeByteArray(3, vBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private static GossipOverview decodeOverview(CodedInputStream in, List<UniqueAddress> addrs) throws IOException {
        Set<UniqueAddress> seen = new HashSet<>();
        List<Reachability.Record> records = new ArrayList<>();
        Map<UniqueAddress, Long> versions = new HashMap<>();
        int tag;
        while ((tag = in.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> seen.add(addrs.get(in.readInt32()));
                case 2 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    int obs = 0;
                    int sub = 0;
                    Reachability.ReachabilityStatus st = Reachability.ReachabilityStatus.Reachable;
                    long ver = 0L;
                    int rTag;
                    while ((rTag = in.readTag()) != 0) {
                        switch (WireFormat.getTagFieldNumber(rTag)) {
                            case 1 -> obs = in.readInt32();
                            case 2 -> sub = in.readInt32();
                            case 3 -> st = Reachability.ReachabilityStatus.values()[in.readInt32()];
                            case 4 -> ver = in.readInt64();
                            default -> in.skipField(rTag);
                        }
                    }
                    in.popLimit(limit);
                    records.add(new Reachability.Record(addrs.get(obs), addrs.get(sub), st, ver));
                }
                case 3 -> {
                    int len = in.readRawVarint32();
                    int limit = in.pushLimit(len);
                    int obs = 0;
                    long ver = 0L;
                    int vTag;
                    while ((vTag = in.readTag()) != 0) {
                        switch (WireFormat.getTagFieldNumber(vTag)) {
                            case 1 -> obs = in.readInt32();
                            case 2 -> ver = in.readInt64();
                            default -> in.skipField(vTag);
                        }
                    }
                    in.popLimit(limit);
                    versions.put(addrs.get(obs), ver);
                }
                default -> in.skipField(tag);
            }
        }
        return new GossipOverview(seen, new Reachability(records, versions));
    }

    private static byte[] encodeVectorClock(VectorClock vc) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<String, Long> e : vc.versions().entrySet()) {
            ByteArrayOutputStream vBaos = new ByteArrayOutputStream();
            CodedOutputStream vOut = CodedOutputStream.newInstance(vBaos);
            vOut.writeString(1, e.getKey());
            vOut.writeInt64(2, e.getValue());
            vOut.flush();
            out.writeByteArray(1, vBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private static VectorClock decodeVectorClock(CodedInputStream in) throws IOException {
        TreeMap<String, Long> map = new TreeMap<>();
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == 1) {
                int len = in.readRawVarint32();
                int limit = in.pushLimit(len);
                String node = "";
                long ts = 0L;
                int vTag;
                while ((vTag = in.readTag()) != 0) {
                    switch (WireFormat.getTagFieldNumber(vTag)) {
                        case 1 -> node = in.readString();
                        case 2 -> ts = in.readInt64();
                        default -> in.skipField(vTag);
                    }
                }
                in.popLimit(limit);
                map.put(node, ts);
            } else {
                in.skipField(tag);
            }
        }
        return new VectorClock(map);
    }
}
