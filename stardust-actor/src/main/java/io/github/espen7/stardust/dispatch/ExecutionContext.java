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
package io.github.espen7.stardust.dispatch;

import java.util.concurrent.Executor;

/**
 * 能报告任务提交失败的执行上下文，对应 Scala 的 ExecutionContext。
 */
public interface ExecutionContext extends Executor {
    /**
     * 报告执行或提交任务时发生的非致命异常。
     *
     * @param cause 需要报告的异常
     */
    void reportFailure(Throwable cause);
}
