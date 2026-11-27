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

import io.github.espen7.tapio.annotation.InternalApi;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * INTERNAL API：分片查询元数据结果封装。
 */
@InternalApi
public final class ShardingQueries {
    private ShardingQueries() {
    }

    /**
     * 分组查询结果与超时/失败分片统计。
     *
     * @param <B> 响应数据类型
     */
    public record ShardsQueryResult<B>(
            Set<String> failed,
            List<B> responses,
            int total,
            Duration timeout) {

        public int queried() {
            return failed.size() + responses.size();
        }

        @Override
        public String toString() {
            if (total == 0) {
                return "Shard region had zero shards to gather metadata from.";
            }
            int q = queried();
            String shardsOf = q < total ? "shards of [" + total + "]:" : "shards:";
            return "Queried [" + q + "] " + shardsOf + " [" + responses.size()
                    + "] responsive, [" + failed.size() + "] failed after " + timeout + ".";
        }
    }
}
