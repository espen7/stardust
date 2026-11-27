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
package io.github.espen7.tapio.cluster.ddata;

import io.github.espen7.tapio.cluster.UniqueAddress;

import java.io.Serial;
import java.io.Serializable;

/**
 * 集群节点的分布式数据唯一标识包装。
 *
 * @param uniqueAddress 集群唯一地址
 */
public record SelfUniqueAddress(UniqueAddress uniqueAddress) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
