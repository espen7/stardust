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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.util.NoStackTrace;

import java.util.concurrent.TimeoutException;

/**
 * {@link Patterns#ask} 在等待回复超时或请求目标已终止时用于完成结果的异常。
 *
 * <p>此异常不采集堆栈跟踪。
 */
public class AskTimeoutException extends TimeoutException implements NoStackTrace {
    private final Throwable cause;

    /**
     * 创建没有关联原因的 ask 超时异常。
     *
     * @param message 描述目标已终止或等待回复超时的诊断消息
     */
    public AskTimeoutException(String message) {
        this(message, null);
    }

    /**
     * 创建带有关联原因的 ask 超时异常。
     *
     * @param message 描述目标已终止或等待回复超时的诊断消息
     * @param cause 关联原因，可以为 {@code null}
     */
    public AskTimeoutException(String message, Throwable cause) {
        super(message);
        this.cause = cause;
    }

    @Override
    public synchronized Throwable getCause() {
        return cause;
    }

    /**
     * 直接返回此异常实例。
     *
     * @return 此异常实例
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
