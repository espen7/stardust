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
package io.github.espen7.tapio.pattern;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Set;

/** {@link PromiseActorRef} 的 VarHandle 字段偏移定义。 */
final class AbstractPromiseActorRef {
    static final VarHandle stateHandle;
    static final VarHandle watchedByHandle;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(PromiseActorRef.class, MethodHandles.lookup());
            stateHandle = lookup.findVarHandle(PromiseActorRef.class, "state", Object.class);
            watchedByHandle = lookup.findVarHandle(PromiseActorRef.class, "watchedBy", Set.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }
}
