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

import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 为 {@link ActorSystem} 生成并持有唯一 {@code longAddressUid} 的扩展。
 */
public final class AddressUidExtension extends AbstractExtensionId<AddressUidExtension.Uid>
        implements ExtensionIdProvider {

    /** 单例实例。 */
    public static final AddressUidExtension INSTANCE = new AddressUidExtension();

    private AddressUidExtension() {
    }

    /**
     * 持有系统 UID 的扩展实例。
     *
     * @param longAddressUid 64 位唯一系统 UID
     */
    public record Uid(long longAddressUid) implements Extension {
        /**
         * 返回 32 位兼容 UID。
         *
         * @return 低 32 位整型 UID
         */
        public int addressUid() {
            return (int) longAddressUid;
        }
    }

    @Override
    public ExtensionId<Uid> lookup() {
        return INSTANCE;
    }

    @Override
    public Uid createExtension(ExtendedActorSystem system) {
        return new Uid(ThreadLocalRandom.current().nextLong());
    }
}
