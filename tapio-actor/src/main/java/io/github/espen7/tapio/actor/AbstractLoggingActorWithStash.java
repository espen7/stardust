/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.actor;

import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

/**
 * 同时支持消息暂存与日志的 Java Actor 基类。
 */
public abstract class AbstractLoggingActorWithStash extends AbstractActorWithStash implements ActorLogging {
    private LoggingAdapter log;

    /** 创建支持消息暂存与日志的 Actor 实例。 */
    protected AbstractLoggingActorWithStash() {
    }

    @Override
    public LoggingAdapter log() {
        if (log == null) {
            log = Logging.getLogger(context.system(), self.path().toString(), getClass());
        }
        return log;
    }

    /**
     * Java API：返回绑定此 Actor 的 {@link LoggingAdapter}。
     *
     * @return LoggingAdapter 实例
     */
    public LoggingAdapter getLog() {
        return log();
    }
}
