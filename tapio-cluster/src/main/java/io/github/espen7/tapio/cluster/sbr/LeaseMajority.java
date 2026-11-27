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

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * INTERNAL API：基于外部 {@code Lease} 仲裁多数派/少数派的 SBR 策略。
 */
@InternalApi
public final class LeaseMajority extends DowningStrategy {

    private final String leaseImplementation;
    private final Duration acquireLeaseDelayForMinority;

    public LeaseMajority(
            String leaseImplementation,
            Duration acquireLeaseDelayForMinority,
            Optional<String> role) {
        super(role);
        this.leaseImplementation = leaseImplementation;
        this.acquireLeaseDelayForMinority = acquireLeaseDelayForMinority;
    }

    public String leaseImplementation() {
        return leaseImplementation;
    }

    public Duration acquireLeaseDelayForMinority() {
        return acquireLeaseDelayForMinority;
    }

    @Override
    protected Decision decide(
            List<Member> eligible,
            List<Member> reachableEligible,
            List<Member> unreachableEligible) {
        if (reachableEligible.size() > unreachableEligible.size()) {
            return Decision.DOWN_UNREACHABLE;
        }
        return Decision.DOWN_REACHABLE;
    }

    /**
     * 判断本侧是否处于少数派分区（相等时也视为需要延迟抢锁）。
     */
    public boolean isInMinority(int reachableCount, int unreachableCount) {
        return reachableCount <= unreachableCount;
    }
}
