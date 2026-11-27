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

/**
 * 与节点间关联（Association）相关的生命周期事件抽象基类。
 */
public sealed abstract class AssociationEvent implements RemotingLifecycleEvent
        permits AssociatedEvent, DisassociatedEvent, AssociationErrorEvent {

    /**
     * 返回本地节点地址。
     *
     * @return 本地地址
     */
    public abstract Address localAddress();

    /**
     * 返回远端节点地址。
     *
     * @return 远端地址
     */
    public abstract Address remoteAddress();

    /**
     * 返回该关联是否为入站连接。
     *
     * @return 入站连接返回 {@code true}
     */
    public abstract boolean inbound();

    /**
     * 返回事件名称前缀。
     *
     * @return 事件名
     */
    protected abstract String eventName();

    /**
     * Java API：获取远端节点地址。
     *
     * @return 远端地址
     */
    public final Address getRemoteAddress() {
        return remoteAddress();
    }

    /**
     * Java API：获取本地节点地址。
     *
     * @return 本地地址
     */
    public final Address getLocalAddress() {
        return localAddress();
    }

    /**
     * Java API：是否为入站连接。
     *
     * @return 入站连接返回 {@code true}
     */
    public final boolean isInbound() {
        return inbound();
    }

    @Override
    public String toString() {
        return eventName() + " [" + localAddress() + "]" + (inbound() ? " <- " : " -> ") + "[" + remoteAddress() + "]";
    }
}
