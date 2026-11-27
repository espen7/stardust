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
package io.github.espen7.tapio.remote.transport;

import io.github.espen7.tapio.TapioException;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;
import io.github.espen7.tapio.util.NoStackTrace;

import java.io.Serial;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 异步网络传输层 SPI。负责初始化底层连接机制、监听入站关联与发起出站关联。
 */
public interface Transport {

    /**
     * 传输层关联事件标记接口。
     */
    interface AssociationEvent extends NoSerializationVerificationNeeded {
    }

    /**
     * 当关联目标地址非法且不可恢复时抛出的异常。
     */
    final class InvalidAssociationException extends TapioException implements NoStackTrace {
        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param msg 错误描述
         * @param cause 根因异常，可为 {@code null}
         */
        public InvalidAssociationException(String msg, Throwable cause) {
            super(msg, cause);
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    /**
     * 收到新入站关联时通知给 {@link AssociationEventListener} 的事件。
     *
     * @param association 入站关联句柄
     */
    record InboundAssociation(AssociationHandle association) implements AssociationEvent {
    }

    /**
     * 监听传输层入站关联事件的回调接口。
     */
    @FunctionalInterface
    interface AssociationEventListener {
        /**
         * 处理传输层关联事件。
         *
         * @param ev 关联事件
         */
        void notify(AssociationEvent ev);
    }

    /**
     * 将普通 {@link ActorRef} 适配为 {@link AssociationEventListener}。
     *
     * @param actor 接收事件消息的 ActorRef
     */
    record ActorAssociationEventListener(ActorRef actor) implements AssociationEventListener {
        @Override
        public void notify(AssociationEvent ev) {
            actor.tell(ev, ActorRef.noSender);
        }
    }

    /**
     * {@link #listen()} 成功后的返回值：绑定后的本地地址与用于注册监听器的 {@link CompletableFuture}。
     *
     * @param listenAddress 实际绑定的本地监听地址
     * @param listenerPromise 消费方必须完成该 promise 以开始接收关联事件
     */
    record ListenResult(Address listenAddress, CompletableFuture<AssociationEventListener> listenerPromise) {
    }

    /**
     * 返回此传输对应的 URL scheme 标识。
     *
     * @return scheme 字符串
     */
    String schemeIdentifier();

    /**
     * 判断此传输实例是否负责投递到目标地址（必须线程安全且非阻塞）。
     *
     * @param address 目标地址
     * @return 负责时返回 {@code true}
     */
    boolean isResponsibleFor(Address address);

    /**
     * 返回此传输支持的最大单帧载荷字节数（至少 32000 字节）。
     *
     * @return 最大帧字节数
     */
    int maximumPayloadBytes();

    /**
     * 异步启动监听以接受入站关联。
     *
     * @return 包含绑定地址与监听器 promise 的阶段
     */
    CompletionStage<ListenResult> listen();

    /**
     * 异步向远端地址建立逻辑双工关联。
     *
     * @param remoteAddress 远端传输地址
     * @return 关联句柄阶段
     */
    CompletionStage<AssociationHandle> associate(Address remoteAddress);

    /**
     * 异步关闭传输层并释放资源（关闭前尽量刷新待写数据）。
     *
     * @return 关闭完成阶段
     */
    CompletionStage<Boolean> shutdown();

    /**
     * 向传输层发送管理命令。
     *
     * @param cmd 管理命令
     * @return 命令是否被处理
     */
    default CompletionStage<Boolean> managementCommand(Object cmd) {
        return CompletableFuture.completedFuture(false);
    }
}
