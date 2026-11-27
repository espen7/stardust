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
 * INTERNAL API：暴露执行器负载指标。
 */
@InternalApi
public interface LoadMetrics {
    /**
     * 判断执行器当前是否处于满载状态。
     *
     * @return 满载返回 {@code true}
     */
    boolean atFullThrottle();
}
