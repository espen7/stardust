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

/**
 * 状态型 CRDT 数据类型基接口，要求合并操作满足交换律、结合律与幂等律。
 */
public interface ReplicatedData {

    /**
     * 将另一份同类型 CRDT 状态单调合并到当前状态，返回合并后的新实例。
     *
     * @param that 另一份副本状态
     * @return 合并后的新状态
     */
    ReplicatedData mergeData(ReplicatedData that);
}
