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

import io.github.espen7.tapio.annotation.DoNotInherit;

import java.util.Map;
import java.util.Set;

/**
 * 集群成员生命周期状态枚举。
 */
@DoNotInherit
public enum MemberStatus {
    /** 正在加入集群。 */
    Joining,
    /** 存在不可达节点时临时升为弱可达就绪状态。 */
    WeaklyUp,
    /** 正常在线状态。 */
    Up,
    /** 正在优雅离开集群。 */
    Leaving,
    /** 所有节点已感知离开，准备退出。 */
    Exiting,
    /** 已被标记为下线（Down）。 */
    Down,
    /** 已从集群成员环移除。 */
    Removed,
    /** 准备关闭（COORDINATED_SHUTDOWN）。 */
    PreparingForShutdown,
    /** 已就绪可关闭。 */
    ReadyForShutdown;

    private static final Map<MemberStatus, Set<MemberStatus>> ALLOWED_TRANSITIONS = Map.of(
            Joining, Set.of(WeaklyUp, Up, Leaving, Down, Removed),
            WeaklyUp, Set.of(Up, Leaving, Down, Removed),
            Up, Set.of(Leaving, Down, Removed, PreparingForShutdown),
            Leaving, Set.of(Exiting, Down, Removed),
            Down, Set.of(Removed),
            Exiting, Set.of(Removed, Down),
            PreparingForShutdown, Set.of(ReadyForShutdown, Removed, Leaving, Down),
            ReadyForShutdown, Set.of(Removed, Leaving, Down),
            Removed, Set.of());

    /**
     * 判断是否允许从当前状态转换到 {@code target} 状态。
     *
     * @param target 目标状态
     * @return 允许转换返回 {@code true}
     */
    public boolean canTransitionTo(MemberStatus target) {
        return ALLOWED_TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    /** Java API：返回 {@link #Joining}。 */
    public static MemberStatus joining() {
        return Joining;
    }

    /** Java API：返回 {@link #WeaklyUp}。 */
    public static MemberStatus weaklyUp() {
        return WeaklyUp;
    }

    /** Java API：返回 {@link #Up}。 */
    public static MemberStatus up() {
        return Up;
    }

    /** Java API：返回 {@link #Leaving}。 */
    public static MemberStatus leaving() {
        return Leaving;
    }

    /** Java API：返回 {@link #Exiting}。 */
    public static MemberStatus exiting() {
        return Exiting;
    }

    /** Java API：返回 {@link #Down}。 */
    public static MemberStatus down() {
        return Down;
    }

    /** Java API：返回 {@link #Removed}。 */
    public static MemberStatus removed() {
        return Removed;
    }
}
