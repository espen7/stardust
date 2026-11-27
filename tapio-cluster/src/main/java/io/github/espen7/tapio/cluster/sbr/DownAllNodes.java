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
 * INTERNAL API：在发生不可达时下线全部节点的 SBR 策略。
 */
@InternalApi
public final class DownAllNodes extends DowningStrategy {

    public DownAllNodes() {
        super(Optional.empty());
    }

    @Override
    protected Decision decide(
            List<Member> eligible,
            List<Member> reachableEligible,
            List<Member> unreachableEligible) {
        return Decision.DOWN_ALL;
    }
}
