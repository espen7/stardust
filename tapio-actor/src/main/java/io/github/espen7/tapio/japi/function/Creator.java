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
package io.github.espen7.tapio.japi.function;

import java.io.Serializable;

/**
 * Actor 实例的延迟创建函数，对应 Pekko japi.function.Creator。
 *
 * @param <T> 创建结果类型
 */
@FunctionalInterface
public interface Creator<T> extends Serializable {
    /**
     * 创建一个 Actor 实例。
     *
     * @return 新 Actor 实例
     * @throws Exception 创建失败时抛出的异常
     */
    T create() throws Exception;
}
