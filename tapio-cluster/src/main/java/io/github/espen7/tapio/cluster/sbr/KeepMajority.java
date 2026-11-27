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

import java.util.List;
import java.util.Optional;

/**
 * INTERNAL API：保留多数派分区、下线少数派分区的 SBR 策略。
 */
@InternalApi
public final class KeepMajority extends DowningStrategy {

    public KeepMajority(Optional<String> role) {
        super(role);
    }

    @Override
    protected Decision decide(
            List<Member> eligible,
            List<Member> reachableEligible,
            List<Member> unreachableEligible) {
        if (reachableEligible.size() > unreachableEligible.size()) {
            return Decision.DOWN_UNREACHABLE;
        }
        if (reachableEligible.size() < unreachableEligible.size()) {
            return Decision.DOWN_REACHABLE;
        }
        return Decision.DOWN_ALL;
    }
}
