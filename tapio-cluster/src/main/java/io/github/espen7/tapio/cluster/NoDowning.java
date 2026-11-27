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

import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;

import java.time.Duration;
import java.util.Optional;

/**
 * 默认的空 DowningProvider 实现，不自动 Down 任何节点。
 */
public final class NoDowning extends DowningProvider {
    private final ClusterSettings settings;

    /**
     * 构造空 DowningProvider。
     *
     * @param system 关联的 {@link ActorSystem}
     */
    public NoDowning(ActorSystem system) {
        super(system);
        this.settings = new ClusterSettings(system.settings().config(), system.settings().name());
    }

    @Override
    public Duration downRemovalMargin() {
        return settings.DownRemovalMargin;
    }

    @Override
    public Optional<Props> downingActorProps() {
        return Optional.empty();
    }
}
