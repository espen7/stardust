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
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;

import java.util.Set;

/**
 * 返回当前 {@link io.github.espen7.tapio.actor.ActorSystem} 已绑定监听地址集合的扩展。
 */
public final class BoundAddressesExtension extends AbstractExtensionId<BoundAddressesExtension.BoundAddresses>
        implements ExtensionIdProvider {

    /** 单例实例。 */
    public static final BoundAddressesExtension INSTANCE = new BoundAddressesExtension();

    private BoundAddressesExtension() {
    }

    /**
     * 已绑定地址集合扩展实例。
     */
    public static final class BoundAddresses implements Extension {
        private final ExtendedActorSystem system;

        private BoundAddresses(ExtendedActorSystem system) {
            this.system = system;
        }

        /**
         * 返回已绑定的传输地址集合。
         *
         * @return 不可变地址集合
         */
        public Set<Address> boundAddresses() {
            if (system.provider() instanceof RemoteActorRefProvider rarp) {
                return rarp.transport().addresses();
            }
            return Set.of(system.provider().getDefaultAddress());
        }
    }

    @Override
    public ExtensionId<BoundAddresses> lookup() {
        return INSTANCE;
    }

    @Override
    public BoundAddresses createExtension(ExtendedActorSystem system) {
        return new BoundAddresses(system);
    }
}
