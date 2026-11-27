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
package io.github.espen7.tapio.actor;

import java.io.Serializable;

/**
 * {@link Deploy} 的作用域标记接口，用于区分本地、远端或集群部署作用域并携带扩展部署信息。
 */
public interface Scope extends Serializable {

    /**
     * 合并两个作用域，{@code this} 优先。
     *
     * @param other 回退作用域
     * @return 合并后的作用域
     */
    Scope withFallback(Scope other);
}
