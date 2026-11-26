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
package io.github.espen7.tapio.dispatch.internal;

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.Batchable;

/**
 * INTERNAL API：识别 Pekko Batchable；Java 边界没有 Scala Batchable 类型。
 */
@InternalApi
public final class ScalaBatchable {
    private ScalaBatchable() {
    }

    /**
     * 判断任务是否允许 batching。
     *
     * @param runnable 待判断的任务
     * @return 任务实现 {@link Batchable} 且允许合并时返回 {@code true}
     */
    public static boolean isBatchable(Runnable runnable) {
        return runnable instanceof Batchable batchable && batchable.isBatchable();
    }
}
