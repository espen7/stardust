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
package io.github.espen7.tapio.remote.transport.netty;

import io.github.espen7.tapio.TapioException;

import java.io.Serial;

/**
 * Netty 传输层异常。
 */
public class NettyTransportException extends TapioException {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 构造 Netty 传输异常。
     *
     * @param msg 错误描述
     * @param cause 根因异常，可为 {@code null}
     */
    public NettyTransportException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * 构造无根因的 Netty 传输异常。
     *
     * @param msg 错误描述
     */
    public NettyTransportException(String msg) {
        this(msg, null);
    }
}
