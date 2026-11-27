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

import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.event.LoggingAdapter;

/**
 * INTERNAL API：向 {@link ActorSystem#eventStream()} 发布远程生命周期事件并按级别记录日志。
 */
@InternalApi
public final class EventPublisher {
    private final ActorSystem system;
    private final LoggingAdapter log;
    private final LogEvent.Level logLevel;

    /**
     * 构造事件发布器。
     *
     * @param system 所属 ActorSystem
     * @param log 日志适配器
     * @param logLevel 生命周期事件输出阈值级别
     */
    public EventPublisher(ActorSystem system, LoggingAdapter log, LogEvent.Level logLevel) {
        this.system = system;
        this.log = log;
        this.logLevel = logLevel;
    }

    /**
     * 发布事件并在满足日志级别时输出日志。
     *
     * @param message 生命周期事件
     */
    public void notifyListeners(RemotingLifecycleEvent message) {
        system.eventStream().publish(message);
        if (logLevel != LogEvent.Level.OFF && message.logLevel().ordinal() >= logLevel.ordinal()) {
            switch (message.logLevel()) {
                case DEBUG -> log.debug("{}", message);
                case INFO -> log.info("{}", message);
                case WARNING -> log.warning("{}", message);
                case ERROR -> log.error("{}", message);
                case OFF -> { }
            }
        }
    }
}
