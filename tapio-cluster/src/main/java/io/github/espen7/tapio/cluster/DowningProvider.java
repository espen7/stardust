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
 * 集群 Downing 策略提供者 SPI，负责在节点不可达时决定是否及何时执行 {@code down}。
 */
public abstract class DowningProvider {
    protected final ActorSystem system;

    /**
     * 构造 DowningProvider。
     *
     * @param system 关联的 {@link ActorSystem}
     */
    protected DowningProvider(ActorSystem system) {
        this.system = system;
    }

    /**
     * 返回应触发节点移除的边际时长（通常对齐 {@code down-removal-margin}）。
     *
     * @return 移除边际时长
     */
    public abstract Duration downRemovalMargin();

    /**
     * 返回负责执行自动 Downing 决策的 Actor {@link Props}（无需自动 Downing 时返回空）。
     *
     * @return 可选的 Downing Actor Props
     */
    public abstract Optional<Props> downingActorProps();
}
