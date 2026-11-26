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
package io.github.espen7.tapio.dispatch;

import io.github.espen7.tapio.annotation.InternalApi;

/**
 * INTERNAL API：可提交给 BatchingExecutor 合并执行的任务。
 */
@InternalApi
public interface Batchable extends Runnable {
    /**
     * 判断当前任务是否允许与其他嵌套任务合并执行。
     *
     * @return 允许合并时返回 {@code true}
     */
    boolean isBatchable();
}
