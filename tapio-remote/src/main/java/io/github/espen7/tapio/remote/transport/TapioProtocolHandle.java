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
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.concurrent.CompletableFuture;

/**
 * INTERNAL API：Tapio 协议层的逻辑关联句柄，封装握手信息并透传 PDU 读写。
 */
@InternalApi
public final class TapioProtocolHandle implements AssociationHandle {
    private final Address localAddress;
    private final Address remoteAddress;
    private final CompletableFuture<HandleEventListener> readHandlerPromise;
    private final AssociationHandle wrappedHandle;
    private final TapioPduCodec.HandshakeInfo handshakeInfo;

    /**
     * 构造协议句柄。
     *
     * @param localAddress 本地地址（含协议 scheme）
     * @param remoteAddress 远端地址（含协议 scheme）
     * @param readHandlerPromise 入站载荷监听器 promise
     * @param wrappedHandle 底层传输句柄
     * @param handshakeInfo 对端握手信息
     */
    public TapioProtocolHandle(
            Address localAddress,
            Address remoteAddress,
            CompletableFuture<HandleEventListener> readHandlerPromise,
            AssociationHandle wrappedHandle,
            TapioPduCodec.HandshakeInfo handshakeInfo) {
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this.readHandlerPromise = readHandlerPromise;
        this.wrappedHandle = wrappedHandle;
        this.handshakeInfo = handshakeInfo;
    }

    /**
     * 返回对端握手信息。
     *
     * @return 握手信息
     */
    public TapioPduCodec.HandshakeInfo handshakeInfo() {
        return handshakeInfo;
    }

    @Override
    public Address localAddress() {
        return localAddress;
    }

    @Override
    public Address remoteAddress() {
        return remoteAddress;
    }

    @Override
    public CompletableFuture<HandleEventListener> readHandlerPromise() {
        return readHandlerPromise;
    }

    @Override
    public boolean write(ByteString payload) {
        return wrappedHandle.write(TapioPduCodec.constructPayload(payload));
    }

    @Override
    public void disassociate(String reason, LoggingAdapter log) {
        disassociate(Unknown.INSTANCE, reason, log);
    }

    /**
     * 向对端发送指定原因的 Disassociate PDU 并关闭底层句柄。
     *
     * @param info 断开原因
     * @param reason 日志原因描述
     * @param log 日志适配器
     */
    public void disassociate(DisassociateInfo info, String reason, LoggingAdapter log) {
        AssociationHandle.super.disassociate(reason, log);
        wrappedHandle.write(TapioPduCodec.constructDisassociate(info));
        wrappedHandle.disassociate(reason, log);
    }
}
