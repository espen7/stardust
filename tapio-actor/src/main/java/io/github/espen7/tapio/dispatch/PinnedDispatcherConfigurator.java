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
import io.github.espen7.tapio.event.Warning;

/** 为 PinnedDispatcher 保留配置，并在每次查找时创建新实例。 */
public class PinnedDispatcherConfigurator extends MessageDispatcherConfigurator {
    private final ThreadPoolConfig threadPoolConfig;

    /**
     * 创建 PinnedDispatcher configurator。
     *
     * @param config 有效配置
     * @param prerequisites Dispatcher 运行时组件
     */
    public PinnedDispatcherConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        ExecutorServiceConfigurator executor = configureExecutor();
        if (executor instanceof ThreadPoolExecutorConfigurator threadPool) {
            this.threadPoolConfig = threadPool.threadPoolConfig();
        } else {
            prerequisites.eventStream().publish(new Warning("PinnedDispatcherConfigurator", getClass(),
                    "PinnedDispatcher [" + config.getString("id")
                            + "] not configured to use ThreadPoolExecutor, falling back to default config."));
            this.threadPoolConfig = ThreadPoolConfig.defaults();
        }
    }

    @Override
    public MessageDispatcher dispatcher() {
        return new PinnedDispatcher(this, null, config().getString("id"),
                config().getDuration("shutdown-timeout"), threadPoolConfig);
    }
}
