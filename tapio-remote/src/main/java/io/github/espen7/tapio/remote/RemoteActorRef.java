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

import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.MinimalActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.io.Serial;
import java.util.Objects;

/**
 * INTERNAL API：指向远端节点上 Actor 的 {@link ActorRef} 实现。
 */
@InternalApi
public class RemoteActorRef extends MinimalActorRef {
    @Serial
    private static final long serialVersionUID = 1L;

    private final RemoteTransport remote;
    private final Address localAddressToUse;
    private final ActorPath path;
    private final InternalActorRef parent;
    private volatile boolean cachedIsTerminated = false;

    /**
     * 构造远端 ActorRef。
     *
     * @param remote 远程传输层
     * @param localAddressToUse 用于联系该远端的本地地址
     * @param path 远端 ActorPath
     * @param parent 父级引用
     */
    public RemoteActorRef(
            RemoteTransport remote,
            Address localAddressToUse,
            ActorPath path,
            InternalActorRef parent) {
        this.remote = Objects.requireNonNull(remote, "remote");
        this.localAddressToUse = Objects.requireNonNull(localAddressToUse, "localAddressToUse");
        this.path = Objects.requireNonNull(path, "path");
        this.parent = parent;
    }

    /**
     * 返回用于联系该远端的本地地址。
     *
     * @return 本地地址
     */
    public Address localAddressToUse() {
        return localAddressToUse;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public ActorRefProvider provider() {
        return remote.provider;
    }

    @Override
    public InternalActorRef getParent() {
        return parent;
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        remote.send(message, ActorRef.noSender, this);
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        if (message == null) {
            throw new IllegalArgumentException("Message is null");
        }
        remote.send(message, sender, this);
    }

    @Override
    public boolean isTerminated() {
        return cachedIsTerminated;
    }

    void markTerminated() {
        this.cachedIsTerminated = true;
    }
}
