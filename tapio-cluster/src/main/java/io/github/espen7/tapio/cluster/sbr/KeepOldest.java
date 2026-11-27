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

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Member;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * INTERNAL API：保留最老（oldest）节点所在分区的 SBR 策略。
 */
@InternalApi
public final class KeepOldest extends DowningStrategy {

    private final boolean downIfAlone;

    public KeepOldest(boolean downIfAlone, Optional<String> role) {
        super(role);
        this.downIfAlone = downIfAlone;
    }

    @Override
    protected Decision decide(
            List<Member> eligible,
            List<Member> reachableEligible,
            List<Member> unreachableEligible) {
        if (eligible.isEmpty()) {
            return Decision.DOWN_ALL;
        }
        Member oldest = eligible.stream()
                .min(Comparator.comparingInt(Member::upNumber)
                        .thenComparing((a, b) -> Member.ADDRESS_ORDERING.compare(a.address(), b.address())))
                .orElse(null);
        boolean oldestReachable = oldest != null && reachableEligible.contains(oldest);
        if (downIfAlone && eligible.size() > 1 && reachableEligible.size() == 1 && oldestReachable) {
            return Decision.DOWN_REACHABLE;
        }
        return oldestReachable ? Decision.DOWN_UNREACHABLE : Decision.DOWN_REACHABLE;
    }
}
