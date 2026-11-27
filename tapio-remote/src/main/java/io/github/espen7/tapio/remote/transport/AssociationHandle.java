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

import com.google.protobuf.ByteString;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.concurrent.CompletableFuture;

/**
 * {@link Transport} 创建的逻辑双工连接句柄。
 */
public interface AssociationHandle {

    /**
     * 句柄事件基接口。
     */
    sealed interface HandleEvent extends NoSerializationVerificationNeeded permits InboundPayload, Disassociated {
    }

    /**
     * 收到远端原始字节载荷的事件。
     *
     * @param payload 原始字节串
     */
    record InboundPayload(ByteString payload) implements HandleEvent {
        @Override
        public String toString() {
            return "InboundPayload(size = " + payload.size() + " bytes)";
        }
    }

    /**
     * 关联断开原因。
     */
    sealed interface DisassociateInfo permits Unknown, Shutdown, Quarantined {
    }

    /** 未知原因断开。 */
    final class Unknown implements DisassociateInfo {
        /** 单例实例。 */
        public static final Unknown INSTANCE = new Unknown();

        private Unknown() {
        }
    }

    /** 远端正常关闭断开。 */
    final class Shutdown implements DisassociateInfo {
        /** 单例实例。 */
        public static final Shutdown INSTANCE = new Shutdown();

        private Shutdown() {
        }
    }

    /** 因隔离导致的断开。 */
    final class Quarantined implements DisassociateInfo {
        /** 单例实例。 */
        public static final Quarantined INSTANCE = new Quarantined();

        private Quarantined() {
        }
    }

    /**
     * 关联断开通知事件。
     *
     * @param info 断开原因
     */
    record Disassociated(DisassociateInfo info) implements HandleEvent, DeadLetterSuppression {
    }

    /**
     * 句柄事件监听器。
     */
    @FunctionalInterface
    interface HandleEventListener {
        /**
         * 处理句柄事件。
         *
         * @param ev 句柄事件
         */
        void notify(HandleEvent ev);
    }

    /**
     * 将普通 {@link ActorRef} 适配为 {@link HandleEventListener}。
     *
     * @param actor 接收事件消息的 ActorRef
     */
    record ActorHandleEventListener(ActorRef actor) implements HandleEventListener {
        @Override
        public void notify(HandleEvent ev) {
            actor.tell(ev, ActorRef.noSender);
        }
    }

    /**
     * 返回本地端点地址。
     *
     * @return 本地地址
     */
    Address localAddress();

    /**
     * 返回远端端点地址。
     *
     * @return 远端地址
     */
    Address remoteAddress();

    /**
     * 返回用于注册入站载荷监听器的 {@link CompletableFuture}。在完成前，底层应缓冲入站数据。
     *
     * @return 监听器 promise
     */
    CompletableFuture<HandleEventListener> readHandlerPromise();

    /**
     * 异步非阻塞写入载荷；必须线程安全。
     *
     * @param payload 待发送字节串
     * @return 通道当前是否接受后续写入；返回 {@code false} 表示写缓冲已满且本次写入被丢弃
     */
    boolean write(ByteString payload);

    /**
     * 关闭底层传输连接（可幂等多次调用）。
     *
     * @param reason 关闭原因
     * @param log 日志适配器
     */
    default void disassociate(String reason, LoggingAdapter log) {
        if (log != null && log.isDebugEnabled()) {
            log.debug("Association between local [{}] and remote [{}] was disassociated because {}",
                    localAddress(), remoteAddress(), reason);
        }
    }
}
