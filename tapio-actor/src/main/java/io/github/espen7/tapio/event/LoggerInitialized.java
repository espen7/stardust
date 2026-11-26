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

/** 默认 logger 完成初始化后发送的响应。 */
public abstract class LoggerInitialized {
    private static final LoggerInitialized Instance = new LoggerInitialized() {
    };

    /** 供自定义 logger 响应类型扩展。 */
    protected LoggerInitialized() {
    }

    /**
     * 返回共享的初始化响应实例。
     *
     * @return {@code LoggerInitialized} 单例
     */
    public static LoggerInitialized getInstance() {
        return Instance;
    }
}
