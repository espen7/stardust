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
package io.github.espen7.tapio;

import java.io.Serial;

/** Tapio runtime 的异常基类。 */
public class TapioException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 创建异常。
     *
     * @param message 诊断消息
     */
    public TapioException(String message) {
        this(message, null);
    }

    /**
     * 创建带原因的异常。
     *
     * @param message 诊断消息
     * @param cause 原始异常，可以为 {@code null}
     */
    public TapioException(String message, Throwable cause) {
        super(message, cause);
    }
}
