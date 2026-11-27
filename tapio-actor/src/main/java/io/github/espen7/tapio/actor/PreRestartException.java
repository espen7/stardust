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

/** preRestart 失败时发布到 EventStream 的包装异常。 */
public final class PreRestartException extends ActorInitializationException {
    PreRestartException(ActorRef actor, Throwable cause, Throwable originalCause, Object message) {
        super(actor,
                "exception in preRestart(" + (originalCause == null ? "null" : originalCause.getClass()) + ", "
                        + (message == null ? "None" : message.getClass()) + ")",
                cause);
    }
}
