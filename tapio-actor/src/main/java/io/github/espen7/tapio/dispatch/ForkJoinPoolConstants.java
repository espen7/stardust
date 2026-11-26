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

/** ForkJoinPool 的 Pekko 默认上限与 keep-alive。 */
final class ForkJoinPoolConstants {
    static final int MaxCap = 0x7FFF;
    static final long DefaultKeepAliveMillis = 60000L;

    private ForkJoinPoolConstants() {
    }
}
