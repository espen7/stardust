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
import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serial;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

/**
 * 表示集群中的一个成员节点（包含唯一地址、状态、角色与加入序号）。
 */
public final class Member implements Comparable<Member>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 按 host 与 port 排序的集群地址比较器。 */
    public static final Comparator<Address> ADDRESS_ORDERING = (a, b) -> {
        if (a == b) return 0;
        String ha = a.host() == null ? "" : a.host();
        String hb = b.host() == null ? "" : b.host();
        int cmp = ha.compareTo(hb);
        if (cmp != 0) return cmp;
        int pa = a.port() == null ? 0 : a.port();
        int pb = b.port() == null ? 0 : b.port();
        return Integer.compare(pa, pb);
    };

    /** 按唯一地址排序的成员比较器。 */
    public static final Comparator<Member> ORDERING = Comparator.comparing(Member::uniqueAddress);

    /**
     * INTERNAL API：Leader 选举排序比较器（Down、Exiting、Joining、WeaklyUp 排在末尾）。
     */
    @InternalApi
    public static final Comparator<Member> LEADER_STATUS_ORDERING = (a, b) -> {
        if (a.status == b.status) {
            return ORDERING.compare(a, b);
        }
        return Integer.compare(statusRankForLeader(a.status), statusRankForLeader(b.status));
    };

    private static int statusRankForLeader(MemberStatus status) {
        return switch (status) {
            case WeaklyUp -> 1;
            case Joining -> 2;
            case Exiting -> 3;
            case Down -> 4;
            default -> 0;
        };
    }

    private final UniqueAddress uniqueAddress;
    private final int upNumber;
    private final MemberStatus status;
    private final Set<String> roles;
    private final String appVersion;
    private final String dataCenter;

    /**
     * INTERNAL API：构造成员实例。
     *
     * @param uniqueAddress 成员唯一地址
     * @param upNumber 加入序号（未 Up 时为 {@link Integer#MAX_VALUE}）
     * @param status 当前状态
     * @param roles 角色集合
     * @param appVersion 应用版本
     */
    @InternalApi
    public Member(UniqueAddress uniqueAddress, int upNumber, MemberStatus status, Set<String> roles, String appVersion) {
        this.uniqueAddress = Objects.requireNonNull(uniqueAddress, "uniqueAddress");
        this.upNumber = upNumber;
        this.status = Objects.requireNonNull(status, "status");
        this.roles = Set.copyOf(Objects.requireNonNull(roles, "roles"));
        this.appVersion = appVersion == null ? "0.1.0" : appVersion;
        this.dataCenter = this.roles.stream()
                .filter(r -> r.startsWith(ClusterSettings.DcRolePrefix))
                .findFirst()
                .map(r -> r.substring(ClusterSettings.DcRolePrefix.length()))
                .orElse(ClusterSettings.DefaultDataCenter);
    }

    /**
     * 创建初始状态为 {@link MemberStatus#Joining} 的成员。
     *
     * @param uniqueAddress 唯一地址
     * @param roles 角色集合
     * @param appVersion 应用版本
     * @return 新成员实例
     */
    @InternalApi
    public static Member create(UniqueAddress uniqueAddress, Set<String> roles, String appVersion) {
        return new Member(uniqueAddress, Integer.MAX_VALUE, MemberStatus.Joining, roles, appVersion);
    }

    /**
     * 创建已移除状态的成员占位。
     *
     * @param node 唯一地址
     * @return {@link MemberStatus#Removed} 状态成员
     */
    @InternalApi
    public static Member removed(UniqueAddress node) {
        return new Member(node, Integer.MAX_VALUE, MemberStatus.Removed,
                Set.of(ClusterSettings.DcRolePrefix + "-N/A"), "0.0.0");
    }

    /** 返回唯一地址。 */
    public UniqueAddress uniqueAddress() {
        return uniqueAddress;
    }

    /** 返回通信地址。 */
    public Address address() {
        return uniqueAddress.address();
    }

    /** INTERNAL API：返回 Up 序号。 */
    @InternalApi
    public int upNumber() {
        return upNumber;
    }

    /** 返回当前成员状态。 */
    public MemberStatus status() {
        return status;
    }

    /** 返回成员角色集合。 */
    public Set<String> roles() {
        return roles;
    }

    /** Java API：返回不可变角色集合。 */
    public Set<String> getRoles() {
        return roles;
    }

    /** 返回所属数据中心名称。 */
    public String dataCenter() {
        return dataCenter;
    }

    /** 返回应用版本。 */
    public String appVersion() {
        return appVersion;
    }

    /** 判断是否包含指定角色。 */
    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    /**
     * 判断当前成员是否比 {@code other} 更早加入集群（同数据中心内比较）。
     *
     * @param other 另一个成员
     * @return 更早加入返回 {@code true}
     */
    public boolean isOlderThan(Member other) {
        if (!dataCenter.equals(other.dataCenter)) {
            throw new IllegalArgumentException(
                    "Comparing members of different data centers with isOlderThan is not allowed. ["
                            + this + "] vs. [" + other + "]");
        }
        if (upNumber == other.upNumber) {
            return ADDRESS_ORDERING.compare(address(), other.address()) < 0;
        }
        return upNumber < other.upNumber;
    }

    /**
     * 返回状态转换后的新成员实例。
     *
     * @param newStatus 目标状态
     * @return 新成员实例
     */
    public Member copy(MemberStatus newStatus) {
        if (newStatus == this.status) {
            return this;
        }
        if (!this.status.canTransitionTo(newStatus)) {
            throw new IllegalArgumentException("Invalid member status transition [ " + this + " -> " + newStatus + "]");
        }
        return new Member(uniqueAddress, upNumber, newStatus, roles, appVersion);
    }

    /**
     * 返回分配了 {@code upNumber} 并转为 {@link MemberStatus#Up} 的新成员实例。
     *
     * @param newUpNumber Up 序号
     * @return Up 状态成员实例
     */
    public Member copyUp(int newUpNumber) {
        return new Member(uniqueAddress, newUpNumber, status, roles, appVersion).copy(MemberStatus.Up);
    }

    @Override
    public int compareTo(Member o) {
        return ORDERING.compare(this, o);
    }

    @Override
    public int hashCode() {
        return uniqueAddress.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Member other && uniqueAddress.equals(other.uniqueAddress);
    }

    @Override
    public String toString() {
        return "Member(" + address() + ", " + status + ")";
    }
}
