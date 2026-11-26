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
package io.github.espen7.tapio.event;

/**
 * 在发布日志事件到 {@link EventStream} 前进行过滤的策略接口，对应 Pekko {@code LoggingFilter}。
 */
public interface LoggingFilter {
    /**
     * 判断指定来源的 ERROR 级别日志是否启用。
     *
     * @param logClass 日志类
     * @param logSource 日志源名称
     * @return 启用返回 {@code true}
     */
    boolean isErrorEnabled(Class<?> logClass, String logSource);

    /**
     * 判断指定来源的 WARNING 级别日志是否启用。
     *
     * @param logClass 日志类
     * @param logSource 日志源名称
     * @return 启用返回 {@code true}
     */
    boolean isWarningEnabled(Class<?> logClass, String logSource);

    /**
     * 判断指定来源的 INFO 级别日志是否启用。
     *
     * @param logClass 日志类
     * @param logSource 日志源名称
     * @return 启用返回 {@code true}
     */
    boolean isInfoEnabled(Class<?> logClass, String logSource);

    /**
     * 判断指定来源的 DEBUG 级别日志是否启用。
     *
     * @param logClass 日志类
     * @param logSource 日志源名称
     * @return 启用返回 {@code true}
     */
    boolean isDebugEnabled(Class<?> logClass, String logSource);
}
