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

/** ActorSelection 未能确认匹配 Actor 时的失败。 */
public final class ActorNotFound extends RuntimeException {
    private final ActorSelection selection;

    /** @param selection 未解析到 Actor 的选择 */
    public ActorNotFound(ActorSelection selection) {
        super("Actor not found for: " + selection);
        this.selection = selection;
    }

    /** @return 未解析到 Actor 的选择 */
    public ActorSelection selection() {
        return selection;
    }
}
