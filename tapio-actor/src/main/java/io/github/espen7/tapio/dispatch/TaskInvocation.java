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

import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.EventStream;

/** 包装普通 Runnable，并在执行结束后释放 Dispatcher inhabitants。 */
final class TaskInvocation implements Runnable {
    private final EventStream eventStream;
    private final Runnable runnable;
    private final Runnable cleanup;

    TaskInvocation(EventStream eventStream, Runnable runnable, Runnable cleanup) {
        this.eventStream = eventStream;
        this.runnable = runnable;
        this.cleanup = cleanup;
    }

    @Override
    public void run() {
        try {
            runnable.run();
        } catch (Throwable error) {
            if (error instanceof VirtualMachineError || error instanceof ThreadDeath || error instanceof LinkageError) {
                throw error;
            }
            DispatchFailureReporting.report(eventStream,
                    new Error("TaskInvocation", TaskInvocation.class,
                            String.valueOf(error.getMessage()), error));
        } finally {
            cleanup.run();
        }
    }
}
