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

import io.github.espen7.tapio.actor.Address;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 由 {@link Address} 与唯一实例 {@code uid} 组成的节点唯一标识。
 *
 * @param address 节点通信地址
 * @param uid 节点实例 UID
 */
public record UniqueAddress(Address address, long uid) implements Comparable<UniqueAddress>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验地址非空。
     *
     * @param address 节点地址
     * @param uid 实例 UID
     */
    public UniqueAddress {
        Objects.requireNonNull(address, "address");
    }

    @Override
    public int hashCode() {
        return Long.hashCode(uid);
    }

    @Override
    public int compareTo(UniqueAddress that) {
        int result = Address.ADDRESS_ORDERING.compare(this.address, that.address);
        if (result == 0) {
            return Long.compare(this.uid, that.uid);
        }
        return result;
    }

    @Override
    public String toString() {
        return address + "#" + uid;
    }
}
