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
package io.github.espen7.tapio.cluster.singleton.protobuf;

import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManager;
import io.github.espen7.tapio.serialization.BaseSerializer;
import io.github.espen7.tapio.serialization.SerializerWithStringManifest;

import java.io.NotSerializableException;

/**
 * 集群单例交接协议消息序列化器。
 */
public final class ClusterSingletonMessageSerializer extends SerializerWithStringManifest implements BaseSerializer {

    private static final String HAND_OVER_TO_ME_MANIFEST = "A";
    private static final String HAND_OVER_IN_PROGRESS_MANIFEST = "B";
    private static final String HAND_OVER_DONE_MANIFEST = "C";
    private static final String TAKE_OVER_FROM_ME_MANIFEST = "D";
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final ExtendedActorSystem system;
    private final int identifier;

    public ClusterSingletonMessageSerializer(ExtendedActorSystem system) {
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
            case ClusterSingletonManager.HandOverToMe ignored -> HAND_OVER_TO_ME_MANIFEST;
            case ClusterSingletonManager.HandOverInProgress ignored -> HAND_OVER_IN_PROGRESS_MANIFEST;
            case ClusterSingletonManager.HandOverDone ignored -> HAND_OVER_DONE_MANIFEST;
            case ClusterSingletonManager.TakeOverFromMe ignored -> TAKE_OVER_FROM_ME_MANIFEST;
            default -> throw new IllegalArgumentException(
                    "Cannot serialize object of type [" + o.getClass().getName() + "]");
        };
    }

    @Override
    public byte[] toBinary(Object o) {
        return EMPTY_BYTES;
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
        return switch (manifest) {
            case HAND_OVER_TO_ME_MANIFEST -> ClusterSingletonManager.HandOverToMe.INSTANCE;
            case HAND_OVER_IN_PROGRESS_MANIFEST -> ClusterSingletonManager.HandOverInProgress.INSTANCE;
            case HAND_OVER_DONE_MANIFEST -> ClusterSingletonManager.HandOverDone.INSTANCE;
            case TAKE_OVER_FROM_ME_MANIFEST -> ClusterSingletonManager.TakeOverFromMe.INSTANCE;
            default -> throw new NotSerializableException("Unknown singleton manifest: " + manifest);
        };
    }
}
