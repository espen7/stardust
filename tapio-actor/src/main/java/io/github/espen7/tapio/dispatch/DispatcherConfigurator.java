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

/** 读取有效配置，并为普通 Dispatcher 保留唯一实例。 */
public class DispatcherConfigurator extends MessageDispatcherConfigurator {
    private final MessageDispatcher instance;

    /**
     * 创建普通 Dispatcher configurator。
     *
     * @param config 有效配置
     * @param prerequisites Dispatcher 运行时组件
     */
    public DispatcherConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        ExecutorServiceConfigurator executor = configureExecutor();
        instance = new Dispatcher(this, config.getString("id"), config.getInt("throughput"),
                config.getDuration("throughput-deadline-time"), executor,
                config.getDuration("shutdown-timeout"));
    }

    @Override
    public MessageDispatcher dispatcher() {
        return instance;
    }
}
