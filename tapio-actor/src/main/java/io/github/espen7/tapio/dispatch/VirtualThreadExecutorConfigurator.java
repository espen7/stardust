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

/**
 * 按 Pekko {@code virtual-thread-executor} 配置创建基于虚拟线程的 {@link java.util.concurrent.ExecutorService}。
 */
public final class VirtualThreadExecutorConfigurator extends ExecutorServiceConfigurator {
    private final int virtualThreadStartNumber;

    /**
     * 创建 virtual-thread-executor configurator。
     *
     * @param config virtual-thread-executor 配置
     * @param prerequisites Dispatcher 运行时组件
     */
    public VirtualThreadExecutorConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        this.virtualThreadStartNumber = config.hasPath("virtual-thread-start-number")
                ? config.getInt("virtual-thread-start-number")
                : -1;
    }

    @Override
    public boolean isVirtualized() {
        return VirtualThreadSupport.isSupported;
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        String prefix = threadFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.name() + "-" + id
                : id;
        ThreadFactory namedFactory = threadFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.withName(monitorable.name() + "-" + id)
                : threadFactory;
        return () -> VirtualThreadSupport.createVirtualized(namedFactory, null, prefix, virtualThreadStartNumber);
    }
}
