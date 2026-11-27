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
package io.github.espen7.tapio.cluster.sbr;

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.cluster.Reachability;
import io.github.espen7.tapio.cluster.UniqueAddress;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitBrainResolverTest {

    private static Member member(int port, int upNumber) {
        UniqueAddress ua = new UniqueAddress(new Address("tapio", "sys", "127.0.0.1", port), port);
        return new Member(ua, upNumber, MemberStatus.Up, Set.of("dc-default"), "1.0.0");
    }

    @Test
    void detectsIndirectlyConnectedNodesFromCycleAndSeenBy() {
        Member m1 = member(2551, 1);
        Member m2 = member(2552, 2);
        Member m3 = member(2553, 3);
        Set<Member> all = Set.of(m1, m2, m3);
        Set<Member> unreachable = Set.of(m2);

        Reachability r = new Reachability(
                List.of(
                        new Reachability.Record(m1.uniqueAddress(), m2.uniqueAddress(),
                                Reachability.ReachabilityStatus.Unreachable, 1L),
                        new Reachability.Record(m2.uniqueAddress(), m3.uniqueAddress(),
                                Reachability.ReachabilityStatus.Unreachable, 1L)),
                Map.of(m1.uniqueAddress(), 1L, m2.uniqueAddress(), 1L));

        KeepMajority strategy = new KeepMajority(Optional.empty());
        strategy.setReachability(r);
        strategy.setSeenBy(Set.of(m1.address()));

        assertTrue(strategy.hasIndirectlyConnected());
        assertEquals(Set.of(m2.uniqueAddress()), strategy.indirectlyConnected());

        List<Member> toDown = strategy.decideAndSelect(all, unreachable);
        assertEquals(1, toDown.size());
        assertEquals(m2, toDown.getFirst());

        strategy.setSeenBy(Set.of(m1.address(), m3.address()));
        assertEquals(Set.of(m2.uniqueAddress(), m3.uniqueAddress()), strategy.indirectlyConnected());
    }
}
