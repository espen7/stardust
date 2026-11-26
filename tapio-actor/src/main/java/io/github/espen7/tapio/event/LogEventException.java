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
package io.github.espen7.tapio.event;

import io.github.espen7.tapio.util.NoStackTrace;

/** 包装 LogEvent 的异常，供 Dispatcher 失败报告路径原样转发。 */
public class LogEventException extends RuntimeException implements NoStackTrace {
    private final LogEvent event;

    /**
     * 创建日志事件异常。
     *
     * @param event 要包装的日志事件
     * @param cause 异常原因
     */
    public LogEventException(LogEvent event, Throwable cause) {
        super(event.toString(), cause);
        this.event = event;
    }

    /**
     * 返回被包装的日志事件。
     *
     * @return 日志事件
     */
    public LogEvent event() {
        return event;
    }
}
