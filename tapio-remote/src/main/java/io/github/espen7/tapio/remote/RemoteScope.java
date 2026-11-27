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
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Scope;

import java.io.Serial;
import java.util.Objects;

/**
 * 远程部署作用域，指定 Actor 部署到的目标节点 {@link Address}。
 *
 * @param node 目标节点地址
 */
public record RemoteScope(Address node) implements Scope {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验目标地址非空。
     *
     * @param node 目标节点地址
     */
    public RemoteScope {
        Objects.requireNonNull(node, "node");
    }

    @Override
    public Scope withFallback(Scope other) {
        return this;
    }
}
