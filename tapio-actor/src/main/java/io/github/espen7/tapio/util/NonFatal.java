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
package io.github.espen7.tapio.util;

/**
 * 对应 Scala {@code scala.util.control.NonFatal} 的 Java 判定。
 *
 * <p>Java 没有 Scala {@code ControlThrowable}，因此只排除 JVM 致命错误、
 * 线程终止、链接错误和中断异常。</p>
 */
public final class NonFatal {
    private NonFatal() {
    }

    /**
     * 判断异常是否可按非致命异常处理。
     *
     * @param cause 待判断的异常
     * @return 非致命时返回 {@code true}
     */
    public static boolean apply(Throwable cause) {
        return !(cause instanceof VirtualMachineError
                || cause instanceof ThreadDeath
                || cause instanceof LinkageError
                || cause instanceof InterruptedException);
    }
}
