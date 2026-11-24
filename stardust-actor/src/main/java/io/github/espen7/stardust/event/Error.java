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
package io.github.espen7.stardust.event;

/**
 * ERROR 级别的结构化日志事件。
 */
public final class Error extends LogEvent {
    /**
     * 创建不带 cause 的 ERROR 事件。
     *
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     */
    public Error(String source, Class<?> sourceClass, String message) {
        this(source, sourceClass, message, (Throwable) null);
    }

    /**
     * 创建带 cause 的 ERROR 事件。
     *
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param cause 异常原因
     */
    public Error(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.ERROR, source, sourceClass, message, cause);
    }

    /**
     * 创建带 marker 的 ERROR 事件。
     *
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param marker 日志 marker
     */
    public Error(String source, Class<?> sourceClass, String message, LogMarker marker) {
        super(Level.ERROR, source, sourceClass, message, null, marker);
    }

    /**
     * 创建带 cause 和 marker 的 ERROR 事件。
     *
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param cause 异常原因
     * @param marker 日志 marker
     */
    public Error(String source, Class<?> sourceClass, String message, Throwable cause, LogMarker marker) {
        super(Level.ERROR, source, sourceClass, message, cause, marker);
    }
}
