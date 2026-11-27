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

import java.io.Serial;

/**
 * 远程传输层关闭事件。
 */
public final class RemotingShutdownEvent implements RemotingLifecycleEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 单例实例。 */
    public static final RemotingShutdownEvent INSTANCE = new RemotingShutdownEvent();

    private RemotingShutdownEvent() {
    }

    @Serial
    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.INFO;
    }

    @Override
    public String toString() {
        return "Remoting shut down";
    }
}
