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

import java.util.concurrent.ThreadFactory;

/** 为指定 Dispatcher id 创建 ExecutorServiceFactory。 */
public interface ExecutorServiceFactoryProvider {
    /**
     * 创建 executor factory。
     *
     * @param id Dispatcher 配置 id
     * @param threadFactory 系统线程工厂
     * @return executor factory
     */
    ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory);

    /**
     * 判断 executor 是否使用 virtual thread。
     *
     * @return 使用 virtual thread 时返回 {@code true}
     */
    default boolean isVirtualized() {
        return false;
    }

    /**
     * 返回 virtual thread 名称的起始编号。
     *
     * @return 起始编号
     */
    default int virtualThreadStartNumber() {
        return 0;
    }
}
