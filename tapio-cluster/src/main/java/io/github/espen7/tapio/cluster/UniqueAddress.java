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
package io.github.espen7.tapio.cluster;

import io.github.espen7.tapio.actor.Address;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 集群成员唯一标识，由 {@link Address} 与随机 {@code longUid} 组成，用于区分同地址重启的不同实例。
 *
 * @param address 节点地址
 * @param longUid 节点实例 UID
 */
public record UniqueAddress(Address address, long longUid) implements Comparable<UniqueAddress>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验地址非空。
     *
     * @param address 节点地址
     * @param longUid 实例 UID
     */
    public UniqueAddress {
        Objects.requireNonNull(address, "address");
    }

    /**
     * 从 {@link io.github.espen7.tapio.remote.UniqueAddress} 转换。
     *
     * @param remoteUniqueAddress 远程模块唯一地址
     * @return 集群唯一地址
     */
    public static UniqueAddress fromRemote(io.github.espen7.tapio.remote.UniqueAddress remoteUniqueAddress) {
        return new UniqueAddress(remoteUniqueAddress.address(), remoteUniqueAddress.uid());
    }

    @Override
    public int hashCode() {
        return Long.hashCode(longUid);
    }

    @Override
    public int compareTo(UniqueAddress that) {
        int result = Member.ADDRESS_ORDERING.compare(this.address, that.address);
        if (result == 0) {
            return Long.compare(this.longUid, that.longUid);
        }
        return result;
    }

    @Override
    public String toString() {
        return "UniqueAddress(" + address + "," + longUid + ")";
    }
}
