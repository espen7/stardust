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
package io.github.espen7.tapio.actor;

/** 消息用于协调 system guardian 的终止。 */
final class SystemGuardian {
    private SystemGuardian() {
    }

    /** sender 请求注册为 termination hook。 */
    enum RegisterTerminationHook { INSTANCE }

    /** 通知已注册的 hook 开始终止。 */
    enum TerminationHook { INSTANCE }

    /** sender 已完成终止任务。 */
    enum TerminationHookDone { INSTANCE }
}
