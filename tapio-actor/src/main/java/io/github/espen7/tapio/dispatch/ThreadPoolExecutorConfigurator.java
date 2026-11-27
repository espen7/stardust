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

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

/** 按 Pekko thread-pool-executor 配置创建 ThreadPoolExecutor。 */
public final class ThreadPoolExecutorConfigurator extends ExecutorServiceConfigurator {
    private final ThreadPoolConfig threadPoolConfig;
    private final boolean isVirtualized;
    private final int virtualThreadStartNumber;

    /**
     * 创建 thread-pool-executor configurator。
     *
     * @param config thread-pool-executor 配置
     * @param prerequisites Dispatcher 运行时组件
     */
    public ThreadPoolExecutorConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        this.isVirtualized = config.getBoolean("virtualize") && VirtualThreadSupport.isSupported;
        this.virtualThreadStartNumber = config.hasPath("virtual-thread-start-number")
                ? config.getInt("virtual-thread-start-number")
                : -1;
        Supplier<BlockingQueue<Runnable>> queueFactory = ThreadPoolConfig.linkedBlockingQueue();
        int queueSize = config.getInt("task-queue-size");
        if (queueSize > 0) {
            queueFactory = switch (config.getString("task-queue-type")) {
                case "array" -> ThreadPoolConfig.arrayBlockingQueue(queueSize, false);
                case "", "linked" -> ThreadPoolConfig.linkedBlockingQueue(queueSize);
                case String other -> throw new IllegalArgumentException(
                        "[" + other + "] is not a valid task-queue-type [array|linked]!");
            };
        }

        int corePoolSize;
        int maxPoolSize;
        if (config.getString("fixed-pool-size").equals("off")) {
            corePoolSize = ThreadPoolConfig.scaledPoolSize(config.getInt("core-pool-size-min"),
                    config.getDouble("core-pool-size-factor"), config.getInt("core-pool-size-max"));
            maxPoolSize = Math.max(corePoolSize, ThreadPoolConfig.scaledPoolSize(config.getInt("max-pool-size-min"),
                    config.getDouble("max-pool-size-factor"), config.getInt("max-pool-size-max")));
        } else {
            corePoolSize = config.getInt("fixed-pool-size");
            maxPoolSize = corePoolSize;
        }
        this.threadPoolConfig = new ThreadPoolConfig(config.getBoolean("allow-core-timeout"), corePoolSize,
                maxPoolSize, Duration.ofMillis(config.getDuration("keep-alive-time").toMillis()), queueFactory,
                ThreadPoolConfig.defaultRejectionPolicy);
    }

    ThreadPoolConfig threadPoolConfig() {
        return threadPoolConfig;
    }

    @Override
    public boolean isVirtualized() {
        return isVirtualized;
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        ExecutorServiceFactory poolFactory = createExecutorServiceFactory(threadPoolConfig, id, threadFactory);
        if (!isVirtualized) {
            return poolFactory;
        }
        ThreadFactory namedFactory = threadFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.withName(monitorable.name() + "-" + id)
                : threadFactory;
        String prefix = namedFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.name()
                : id;
        return () -> VirtualThreadSupport.createVirtualized(namedFactory, poolFactory.createExecutorService(),
                prefix, virtualThreadStartNumber);
    }

    static ExecutorServiceFactory createExecutorServiceFactory(ThreadPoolConfig config, String id,
                                                               ThreadFactory threadFactory) {
        ThreadFactory namedFactory = threadFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.withName(monitorable.name() + "-" + id)
                : threadFactory;
        return () -> {
            ThreadPoolExecutor pool = new ThreadPoolExecutorWithMetrics(config.corePoolSize(),
                    config.maxPoolSize(), config.threadTimeout().toNanos(),
                    java.util.concurrent.TimeUnit.NANOSECONDS, config.queueFactory().get(), namedFactory,
                    config.rejectionPolicy());
            pool.allowCoreThreadTimeOut(config.allowCorePoolTimeout());
            return pool;
        };
    }

    private static final class ThreadPoolExecutorWithMetrics extends ThreadPoolExecutor implements LoadMetrics {
        private ThreadPoolExecutorWithMetrics(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                              java.util.concurrent.TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                              ThreadFactory threadFactory,
                                              java.util.concurrent.RejectedExecutionHandler handler) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        @Override
        public boolean atFullThrottle() {
            return getActiveCount() >= getPoolSize();
        }
    }
}
