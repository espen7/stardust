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
package io.github.espen7.tapio.cluster.sharding;

import com.typesafe.config.Config;
import io.github.espen7.tapio.annotation.InternalApi;

import java.time.Duration;
import java.util.Set;

/**
 * INTERNAL API：{@link ClusterShardingHealthCheck} 配置参数。
 */
@InternalApi
public record ClusterShardingHealthCheckSettings(
        Set<String> names,
        Duration timeout,
        Duration disabledAfter) {

    public static ClusterShardingHealthCheckSettings create(Config config) {
        return new ClusterShardingHealthCheckSettings(
                Set.copyOf(config.getStringList("names")),
                config.getDuration("timeout"),
                config.getDuration("disabled-after"));
    }
}
