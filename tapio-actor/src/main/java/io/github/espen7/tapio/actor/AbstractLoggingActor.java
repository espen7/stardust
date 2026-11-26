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

/** 使用当前 Actor path 和 class 作为日志来源的 Classic Actor。 */
public abstract class AbstractLoggingActor extends AbstractActor {
    private LoggingAdapter log;

    /** @return 当前 Actor 的日志适配器 */
    public LoggingAdapter log() {
        if (log == null) log = Logging.getLogger(context.system().eventStream(), self.path().toString(), getClass());
        return log;
    }
}
