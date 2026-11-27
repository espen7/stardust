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

import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.annotation.InternalStableApi;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * INTERNAL API：负责收发远程消息的传输抽象基类。
 */
@InternalApi
public abstract class RemoteTransport {
    protected final ExtendedActorSystem system;
    protected final RemoteActorRefProvider provider;

    /**
     * 构造远程传输实例。
     *
     * @param system 所属 ActorSystem
     * @param provider 关联的 {@link RemoteActorRefProvider}
     */
    protected RemoteTransport(ExtendedActorSystem system, RemoteActorRefProvider provider) {
        this.system = system;
        this.provider = provider;
    }

    /**
     * 关闭远程传输层。
     *
     * @return 关闭完成通知
     */
    public abstract CompletionStage<Done> shutdown();

    /**
     * 返回当前传输层监听的所有地址集合。
     *
     * @return 监听地址集合
     */
    public abstract Set<Address> addresses();

    /**
     * 返回默认传输地址。
     *
     * @return 默认监听地址
     */
    public abstract Address defaultAddress();

    /**
     * 解析用于联系指定远端地址的本地地址。
     *
     * @param remote 远端地址
     * @return 对应的本地地址
     */
    public abstract Address localAddressForRemote(Address remote);

    /**
     * 启动传输层并开始接收连接。
     */
    public abstract void start();

    /**
     * 向指定远程 {@link RemoteActorRef} 发送消息。
     *
     * @param message 消息载荷
     * @param sender 发送者（可为 {@link ActorRef#noSender()}）
     * @param recipient 远程接收方引用
     */
    public abstract void send(Object message, ActorRef sender, RemoteActorRef recipient);

    /**
     * 向底层传输栈发送管理命令。
     *
     * @param cmd 管理命令
     * @return 命令是否被处理
     */
    public CompletionStage<Boolean> managementCommand(Object cmd) {
        return CompletableFuture.completedFuture(false);
    }

    /**
     * 返回传输层日志适配器。
     *
     * @return 日志适配器
     */
    public abstract LoggingAdapter log();

    /**
     * 将远端节点标记为隔离状态，阻止在隔离期内重连。
     *
     * @param address 远端节点地址
     * @param uid 远端节点 UID（为空时执行弱隔离/门控）
     * @param reason 隔离原因
     */
    @InternalStableApi
    public abstract void quarantine(Address address, Optional<Long> uid, String reason);

    /**
     * 返回本地 ActorSystem 的 UID。
     *
     * @return 系统 UID
     */
    public abstract long systemUid();
}
