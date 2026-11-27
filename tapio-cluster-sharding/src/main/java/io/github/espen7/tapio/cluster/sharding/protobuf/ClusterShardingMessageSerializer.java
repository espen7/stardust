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
package io.github.espen7.tapio.cluster.sharding.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.cluster.sharding.Shard;
import io.github.espen7.tapio.cluster.sharding.ShardCoordinator;
import io.github.espen7.tapio.cluster.sharding.ShardRegion;
import io.github.espen7.tapio.cluster.sharding.external.ShardLocations;
import io.github.espen7.tapio.remote.WireFormats;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.Serialization;
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
import java.util.Set;

/**
 * 集群分片协议与协调器状态序列化器。
 */
public class ClusterShardingMessageSerializer extends SerializerWithStringManifest implements BaseSerializer {

    private static final String COORDINATOR_STATE_MANIFEST = "AA";
    private static final String REGISTER_MANIFEST = "AB";
    private static final String REGISTER_PROXY_MANIFEST = "AC";
    private static final String REGISTER_ACK_MANIFEST = "AD";
    private static final String GET_SHARD_HOME_MANIFEST = "AE";
    private static final String SHARD_HOME_MANIFEST = "AF";
    private static final String HOST_SHARD_MANIFEST = "AG";
    private static final String SHARD_STARTED_MANIFEST = "AH";
    private static final String BEGIN_HANDOFF_MANIFEST = "AI";
    private static final String BEGIN_HANDOFF_ACK_MANIFEST = "AJ";
    private static final String HANDOFF_MANIFEST = "AK";
    private static final String SHARD_STOPPED_MANIFEST = "AL";
    private static final String SHARD_REGION_TERMINATED_MANIFEST = "AM";
    private static final String SHARD_HOMES_MANIFEST = "AN";
    private static final String START_ENTITY_MANIFEST = "AO";
    private static final String START_ENTITY_ACK_MANIFEST = "AP";
    private static final String SHARD_STATS_MANIFEST = "AQ";
    private static final String GET_SHARD_REGION_STATS_MANIFEST = "AR";
    private static final String SHARD_REGION_STATS_MANIFEST = "AS";
    private static final String GET_CURRENT_REGIONS_MANIFEST = "AT";
    private static final String CURRENT_REGIONS_MANIFEST = "AU";
    private static final String SHARD_LOCATIONS_MANIFEST = "AV";

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final ExtendedActorSystem system;
    private final int identifier;

    public ClusterShardingMessageSerializer(ExtendedActorSystem system) {
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
            case ShardCoordinator.State ignored -> COORDINATOR_STATE_MANIFEST;
            case ShardCoordinator.Internal.Register ignored -> REGISTER_MANIFEST;
            case ShardCoordinator.Internal.RegisterProxy ignored -> REGISTER_PROXY_MANIFEST;
            case ShardCoordinator.Internal.RegisterAck ignored -> REGISTER_ACK_MANIFEST;
            case ShardCoordinator.Internal.GetShardHome ignored -> GET_SHARD_HOME_MANIFEST;
            case ShardCoordinator.Internal.ShardHome ignored -> SHARD_HOME_MANIFEST;
            case ShardCoordinator.Internal.HostShard ignored -> HOST_SHARD_MANIFEST;
            case ShardCoordinator.Internal.ShardStarted ignored -> SHARD_STARTED_MANIFEST;
            case ShardCoordinator.Internal.BeginHandOff ignored -> BEGIN_HANDOFF_MANIFEST;
            case ShardCoordinator.Internal.BeginHandOffAck ignored -> BEGIN_HANDOFF_ACK_MANIFEST;
            case ShardCoordinator.Internal.HandOff ignored -> HANDOFF_MANIFEST;
            case ShardCoordinator.Internal.ShardStopped ignored -> SHARD_STOPPED_MANIFEST;
            case ShardCoordinator.Internal.ShardRegionTerminated ignored -> SHARD_REGION_TERMINATED_MANIFEST;
            case ShardCoordinator.Internal.ShardHomes ignored -> SHARD_HOMES_MANIFEST;
            case ShardRegion.StartEntity ignored -> START_ENTITY_MANIFEST;
            case ShardRegion.StartEntityAck ignored -> START_ENTITY_ACK_MANIFEST;
            case Shard.ShardStats ignored -> SHARD_STATS_MANIFEST;
            case ShardRegion.GetShardRegionStats ignored -> GET_SHARD_REGION_STATS_MANIFEST;
            case ShardRegion.ShardRegionStats ignored -> SHARD_REGION_STATS_MANIFEST;
            case ShardRegion.GetCurrentRegions ignored -> GET_CURRENT_REGIONS_MANIFEST;
            case ShardRegion.CurrentRegions ignored -> CURRENT_REGIONS_MANIFEST;
            case ShardLocations ignored -> SHARD_LOCATIONS_MANIFEST;
            default -> throw new IllegalArgumentException("Unsupported sharding message: " + o.getClass().getName());
        };
    }

    @Override
    public byte[] toBinary(Object o) {
        try {
            return switch (o) {
                case ShardCoordinator.State s -> encodeCoordinatorState(s);
                case ShardCoordinator.Internal.Register r -> encodeActorRef(r.shardRegion());
                case ShardCoordinator.Internal.RegisterProxy r -> encodeActorRef(r.shardRegionProxy());
                case ShardCoordinator.Internal.RegisterAck ack -> encodeActorRef(ack.coordinator());
                case ShardCoordinator.Internal.GetShardHome g -> encodeShardId(g.shard());
                case ShardCoordinator.Internal.ShardHome h -> encodeShardHome(h.shard(), h.ref());
                case ShardCoordinator.Internal.HostShard h -> encodeShardId(h.shard());
                case ShardCoordinator.Internal.ShardStarted s -> encodeShardId(s.shard());
                case ShardCoordinator.Internal.BeginHandOff b -> encodeShardId(b.shard());
                case ShardCoordinator.Internal.BeginHandOffAck b -> encodeShardId(b.shard());
                case ShardCoordinator.Internal.HandOff h -> encodeShardId(h.shard());
                case ShardCoordinator.Internal.ShardStopped s -> encodeShardId(s.shard());
                case ShardCoordinator.Internal.ShardRegionTerminated t -> encodeActorRef(t.region());
                case ShardCoordinator.Internal.ShardHomes homes -> encodeShardHomes(homes);
                case ShardRegion.StartEntity se -> encodeShardId(se.entityId());
                case ShardRegion.StartEntityAck ack -> encodeShardHomePair(ack.entityId(), ack.shardId());
                case Shard.ShardStats st -> encodeShardStats(st);
                case ShardRegion.GetShardRegionStats ignored -> EMPTY_BYTES;
                case ShardRegion.ShardRegionStats st -> encodeShardRegionStats(st);
                case ShardRegion.GetCurrentRegions ignored -> EMPTY_BYTES;
                case ShardRegion.CurrentRegions cr -> encodeCurrentRegions(cr);
                case ShardLocations sl -> encodeShardLocations(sl);
                default -> throw new IllegalArgumentException(
                        "Unsupported sharding message: " + o.getClass().getName());
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
                case COORDINATOR_STATE_MANIFEST -> decodeCoordinatorState(in);
                case REGISTER_MANIFEST -> new ShardCoordinator.Internal.Register(decodeActorRef(in));
                case REGISTER_PROXY_MANIFEST -> new ShardCoordinator.Internal.RegisterProxy(decodeActorRef(in));
                case REGISTER_ACK_MANIFEST -> new ShardCoordinator.Internal.RegisterAck(decodeActorRef(in));
                case GET_SHARD_HOME_MANIFEST -> new ShardCoordinator.Internal.GetShardHome(decodeShardId(in));
                case SHARD_HOME_MANIFEST -> decodeShardHome(in);
                case HOST_SHARD_MANIFEST -> new ShardCoordinator.Internal.HostShard(decodeShardId(in));
                case SHARD_STARTED_MANIFEST -> new ShardCoordinator.Internal.ShardStarted(decodeShardId(in));
                case BEGIN_HANDOFF_MANIFEST -> new ShardCoordinator.Internal.BeginHandOff(decodeShardId(in));
                case BEGIN_HANDOFF_ACK_MANIFEST -> new ShardCoordinator.Internal.BeginHandOffAck(decodeShardId(in));
                case HANDOFF_MANIFEST -> new ShardCoordinator.Internal.HandOff(decodeShardId(in));
                case SHARD_STOPPED_MANIFEST -> new ShardCoordinator.Internal.ShardStopped(decodeShardId(in));
                case SHARD_REGION_TERMINATED_MANIFEST ->
                        new ShardCoordinator.Internal.ShardRegionTerminated(decodeActorRef(in));
                case SHARD_HOMES_MANIFEST -> decodeShardHomes(in);
                case START_ENTITY_MANIFEST -> new ShardRegion.StartEntity(decodeShardId(in));
                case START_ENTITY_ACK_MANIFEST -> decodeStartEntityAck(in);
                case SHARD_STATS_MANIFEST -> decodeShardStats(in);
                case GET_SHARD_REGION_STATS_MANIFEST -> ShardRegion.GetShardRegionStats.INSTANCE;
                case SHARD_REGION_STATS_MANIFEST -> decodeShardRegionStats(in);
                case GET_CURRENT_REGIONS_MANIFEST -> ShardRegion.GetCurrentRegions.INSTANCE;
                case CURRENT_REGIONS_MANIFEST -> decodeCurrentRegions(in);
                case SHARD_LOCATIONS_MANIFEST -> decodeShardLocations(in);
                default -> throw new NotSerializableException("Unknown sharding manifest: " + manifest);
            };
        } catch (IOException e) {
            NotSerializableException nse = new NotSerializableException(
                    "Failed to deserialize sharding message [" + manifest + "]: " + e.getMessage());
            nse.initCause(e);
            throw nse;
        }
    }

    private byte[] encodeActorRef(ActorRef ref) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, Serialization.serializedActorPath(ref));
        out.flush();
        return baos.toByteArray();
    }

    private ActorRef decodeActorRef(CodedInputStream in) throws IOException {
        String path = "";
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                path = in.readString();
            } else {
                in.skipField(tag);
            }
        }
        return system.provider().resolveActorRef(path);
    }

    private byte[] encodeShardId(String shardId) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, shardId);
        out.flush();
        return baos.toByteArray();
    }

    private String decodeShardId(CodedInputStream in) throws IOException {
        String shardId = "";
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                shardId = in.readString();
            } else {
                in.skipField(tag);
            }
        }
        return shardId;
    }

    private byte[] encodeShardHome(String shard, ActorRef ref) throws IOException {
        return encodeShardHomePair(shard, Serialization.serializedActorPath(ref));
    }

    private byte[] encodeShardHomePair(String first, String second) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, first);
        out.writeString(2, second);
        out.flush();
        return baos.toByteArray();
    }

    private ShardCoordinator.Internal.ShardHome decodeShardHome(CodedInputStream in) throws IOException {
        String shard = "";
        String refPath = "";
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> shard = in.readString();
                case 2 -> refPath = in.readString();
                default -> in.skipField(tag);
            }
        }
        return new ShardCoordinator.Internal.ShardHome(shard, system.provider().resolveActorRef(refPath));
    }

    private ShardRegion.StartEntityAck decodeStartEntityAck(CodedInputStream in) throws IOException {
        String entityId = "";
        String shardId = "";
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> entityId = in.readString();
                case 2 -> shardId = in.readString();
                default -> in.skipField(tag);
            }
        }
        return new ShardRegion.StartEntityAck(entityId, shardId);
    }

    private byte[] encodeShardHomes(ShardCoordinator.Internal.ShardHomes homes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<ActorRef, List<String>> entry : homes.homes().entrySet()) {
            ByteArrayOutputStream eBaos = new ByteArrayOutputStream();
            CodedOutputStream eOut = CodedOutputStream.newInstance(eBaos);
            eOut.writeString(1, Serialization.serializedActorPath(entry.getKey()));
            for (String shardId : entry.getValue()) {
                eOut.writeString(2, shardId);
            }
            eOut.flush();
            out.writeByteArray(1, eBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private ShardCoordinator.Internal.ShardHomes decodeShardHomes(CodedInputStream in) throws IOException {
        Map<ActorRef, List<String>> homes = new HashMap<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                CodedInputStream eIn = CodedInputStream.newInstance(in.readByteArray());
                String regionPath = "";
                List<String> shardIds = new ArrayList<>();
                while (!eIn.isAtEnd()) {
                    int eTag = eIn.readTag();
                    switch (eTag >>> 3) {
                        case 1 -> regionPath = eIn.readString();
                        case 2 -> shardIds.add(eIn.readString());
                        default -> eIn.skipField(eTag);
                    }
                }
                homes.put(system.provider().resolveActorRef(regionPath), List.copyOf(shardIds));
            } else {
                in.skipField(tag);
            }
        }
        return new ShardCoordinator.Internal.ShardHomes(homes);
    }

    private byte[] encodeCoordinatorState(ShardCoordinator.State state) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<String, ActorRef> entry : state.shards().entrySet()) {
            ByteArrayOutputStream eBaos = new ByteArrayOutputStream();
            CodedOutputStream eOut = CodedOutputStream.newInstance(eBaos);
            eOut.writeString(1, entry.getKey());
            eOut.writeString(2, Serialization.serializedActorPath(entry.getValue()));
            eOut.flush();
            out.writeByteArray(1, eBaos.toByteArray());
        }
        for (ActorRef region : state.regions().keySet()) {
            out.writeString(2, Serialization.serializedActorPath(region));
        }
        for (ActorRef proxy : state.regionProxies()) {
            out.writeString(3, Serialization.serializedActorPath(proxy));
        }
        for (String unallocated : state.unallocatedShards()) {
            out.writeString(4, unallocated);
        }
        out.flush();
        return baos.toByteArray();
    }

    private ShardCoordinator.State decodeCoordinatorState(CodedInputStream in) throws IOException {
        Map<String, ActorRef> shards = new HashMap<>();
        Map<ActorRef, List<String>> regions = new HashMap<>();
        Set<ActorRef> proxies = new HashSet<>();
        Set<String> unallocated = new HashSet<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> {
                    CodedInputStream eIn = CodedInputStream.newInstance(in.readByteArray());
                    String shardId = "";
                    String refPath = "";
                    while (!eIn.isAtEnd()) {
                        int eTag = eIn.readTag();
                        switch (eTag >>> 3) {
                            case 1 -> shardId = eIn.readString();
                            case 2 -> refPath = eIn.readString();
                            default -> eIn.skipField(eTag);
                        }
                    }
                    ActorRef ref = system.provider().resolveActorRef(refPath);
                    shards.put(shardId, ref);
                    regions.computeIfAbsent(ref, k -> new ArrayList<>()).add(shardId);
                }
                case 2 -> {
                    ActorRef region = system.provider().resolveActorRef(in.readString());
                    regions.computeIfAbsent(region, k -> new ArrayList<>());
                }
                case 3 -> proxies.add(system.provider().resolveActorRef(in.readString()));
                case 4 -> unallocated.add(in.readString());
                default -> in.skipField(tag);
            }
        }
        Map<ActorRef, List<String>> immutableRegions = new HashMap<>();
        for (Map.Entry<ActorRef, List<String>> entry : regions.entrySet()) {
            immutableRegions.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return new ShardCoordinator.State(
                Map.copyOf(shards),
                Map.copyOf(immutableRegions),
                Set.copyOf(proxies),
                Set.copyOf(unallocated));
    }

    private byte[] encodeShardStats(Shard.ShardStats stats) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, stats.shardId());
        out.writeInt32(2, stats.entityCount());
        out.flush();
        return baos.toByteArray();
    }

    private Shard.ShardStats decodeShardStats(CodedInputStream in) throws IOException {
        String shardId = "";
        int count = 0;
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> shardId = in.readString();
                case 2 -> count = in.readInt32();
                default -> in.skipField(tag);
            }
        }
        return new Shard.ShardStats(shardId, count);
    }

    private byte[] encodeShardRegionStats(ShardRegion.ShardRegionStats stats) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<String, Integer> entry : stats.stats().entrySet()) {
            ByteArrayOutputStream eBaos = new ByteArrayOutputStream();
            CodedOutputStream eOut = CodedOutputStream.newInstance(eBaos);
            eOut.writeString(1, entry.getKey());
            eOut.writeInt32(2, entry.getValue());
            eOut.flush();
            out.writeByteArray(1, eBaos.toByteArray());
        }
        for (String failed : stats.failed()) {
            out.writeString(2, failed);
        }
        out.flush();
        return baos.toByteArray();
    }

    private ShardRegion.ShardRegionStats decodeShardRegionStats(CodedInputStream in) throws IOException {
        Map<String, Integer> stats = new HashMap<>();
        Set<String> failed = new HashSet<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            switch (tag >>> 3) {
                case 1 -> {
                    CodedInputStream eIn = CodedInputStream.newInstance(in.readByteArray());
                    String k = "";
                    int v = 0;
                    while (!eIn.isAtEnd()) {
                        int eTag = eIn.readTag();
                        switch (eTag >>> 3) {
                            case 1 -> k = eIn.readString();
                            case 2 -> v = eIn.readInt32();
                            default -> eIn.skipField(eTag);
                        }
                    }
                    stats.put(k, v);
                }
                case 2 -> failed.add(in.readString());
                default -> in.skipField(tag);
            }
        }
        return new ShardRegion.ShardRegionStats(stats, failed);
    }

    private byte[] encodeCurrentRegions(ShardRegion.CurrentRegions cr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Address addr : cr.regions()) {
            ByteArrayOutputStream addrBaos = new ByteArrayOutputStream();
            CodedOutputStream addrOut = CodedOutputStream.newInstance(addrBaos);
            WireFormats.writeAddressData(addrOut, addr);
            addrOut.flush();
            out.writeByteArray(1, addrBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private ShardRegion.CurrentRegions decodeCurrentRegions(CodedInputStream in) throws IOException {
        Set<Address> regions = new HashSet<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                regions.add(WireFormats.readAddressData(CodedInputStream.newInstance(in.readByteArray())));
            } else {
                in.skipField(tag);
            }
        }
        return new ShardRegion.CurrentRegions(regions);
    }

    private byte[] encodeShardLocations(ShardLocations sl) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        for (Map.Entry<String, Address> entry : sl.locations().entrySet()) {
            ByteArrayOutputStream eBaos = new ByteArrayOutputStream();
            CodedOutputStream eOut = CodedOutputStream.newInstance(eBaos);
            eOut.writeString(1, entry.getKey());
            ByteArrayOutputStream addrBaos = new ByteArrayOutputStream();
            CodedOutputStream addrOut = CodedOutputStream.newInstance(addrBaos);
            WireFormats.writeAddressData(addrOut, entry.getValue());
            addrOut.flush();
            eOut.writeByteArray(2, addrBaos.toByteArray());
            eOut.flush();
            out.writeByteArray(1, eBaos.toByteArray());
        }
        out.flush();
        return baos.toByteArray();
    }

    private ShardLocations decodeShardLocations(CodedInputStream in) throws IOException {
        Map<String, Address> locs = new HashMap<>();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            if ((tag >>> 3) == 1) {
                CodedInputStream eIn = CodedInputStream.newInstance(in.readByteArray());
                String shardId = "";
                Address addr = null;
                while (!eIn.isAtEnd()) {
                    int eTag = eIn.readTag();
                    switch (eTag >>> 3) {
                        case 1 -> shardId = eIn.readString();
                        case 2 -> addr = WireFormats.readAddressData(CodedInputStream.newInstance(eIn.readByteArray()));
                        default -> eIn.skipField(eTag);
                    }
                }
                if (!shardId.isEmpty() && addr != null) {
                    locs.put(shardId, addr);
                }
            } else {
                in.skipField(tag);
            }
        }
        return new ShardLocations(locs);
    }
}
