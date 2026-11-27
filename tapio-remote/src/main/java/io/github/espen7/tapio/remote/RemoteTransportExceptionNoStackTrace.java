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

import io.github.espen7.tapio.util.NoStackTrace;

import java.io.Serial;

/**
 * 不采集堆栈的 {@link RemoteTransportException}。
 */
public class RemoteTransportExceptionNoStackTrace extends RemoteTransportException implements NoStackTrace {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 构造无堆栈远程传输异常。
     *
     * @param message 错误描述
     * @param cause 根因异常，可为 {@code null}
     */
    public RemoteTransportExceptionNoStackTrace(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
