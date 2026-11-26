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

import com.typesafe.config.Config;

import java.util.concurrent.ThreadFactory;

/** 根据 Dispatcher 配置和运行时组件创建 ExecutorService 工厂。 */
public abstract class ExecutorServiceConfigurator implements ExecutorServiceFactoryProvider {
    protected final Config config;
    protected final DispatcherPrerequisites prerequisites;

    /**
     * 保存 executor 配置和构造工厂所需的运行时组件。
     *
     * @param config executor 配置
     * @param prerequisites Dispatcher 运行时组件
     */
    protected ExecutorServiceConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        this.config = config;
        this.prerequisites = prerequisites;
    }

    @Override
    public abstract ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory);
}
