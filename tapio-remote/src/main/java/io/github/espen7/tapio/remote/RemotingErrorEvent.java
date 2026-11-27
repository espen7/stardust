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

import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.event.Logging;

import java.io.Serial;
import java.util.Objects;

/**
 * 远程传输层错误事件。
 *
 * @param cause 异常原因
 */
public record RemotingErrorEvent(Throwable cause) implements RemotingLifecycleEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验异常非空。
     *
     * @param cause 异常原因
     */
    public RemotingErrorEvent {
        Objects.requireNonNull(cause, "cause");
    }

    /**
     * Java API：获取异常原因。
     *
     * @return 异常原因
     */
    public Throwable getCause() {
        return cause;
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.ERROR;
    }

    @Override
    public String toString() {
        return "Remoting error: [" + cause.getMessage() + "] [" + Logging.stackTraceFor(cause) + "]";
    }
}
