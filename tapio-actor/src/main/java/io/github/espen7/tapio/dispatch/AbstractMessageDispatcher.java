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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/** 承载 MessageDispatcher 的 volatile 状态字段句柄。 */
abstract class AbstractMessageDispatcher extends BatchingExecutor {
    static final VarHandle shutdownScheduleHandle;
    static final VarHandle inhabitantsHandle;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(MessageDispatcher.class, MethodHandles.lookup());
            shutdownScheduleHandle = lookup.findVarHandle(
                    MessageDispatcher.class, "_shutdownScheduleDoNotCallMeDirectly", int.class);
            inhabitantsHandle = lookup.findVarHandle(
                    MessageDispatcher.class, "_inhabitantsDoNotCallMeDirectly", long.class);
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }
}
