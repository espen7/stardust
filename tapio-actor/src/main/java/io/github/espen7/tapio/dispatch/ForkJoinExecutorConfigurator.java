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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** 按 Pekko fork-join-executor 配置创建 ForkJoinPool。 */
public final class ForkJoinExecutorConfigurator extends ExecutorServiceConfigurator {
    static int resolveMinimumRunnable(int configured, int parallelism, int jdkMajorVersion) {
        if (configured >= 0) return configured;
        return jdkMajorVersion >= 25 ? Math.min(8, Math.max(1, parallelism / 2)) : 1;
    }

    private final int parallelism;
    private final boolean asyncMode;
    private final int maximumPoolSize;
    private final int minimumRunnable;
    private final boolean isVirtualized;
    private final int virtualThreadStartNumber;

    /**
     * 创建 fork-join-executor configurator。
     *
     * @param config fork-join-executor 配置
     * @param prerequisites Dispatcher 运行时组件
     */
    public ForkJoinExecutorConfigurator(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        this.isVirtualized = config.getBoolean("virtualize") && VirtualThreadSupport.isSupported;
        this.virtualThreadStartNumber = config.hasPath("virtual-thread-start-number")
                ? config.getInt("virtual-thread-start-number")
                : -1;
        this.parallelism = ThreadPoolConfig.scaledPoolSize(config.getInt("parallelism-min"),
                config.getDouble("parallelism-factor"), config.getInt("parallelism-max"));
        this.asyncMode = switch (config.getString("task-peeking-mode")) {
            case "FIFO" -> true;
            case "LIFO" -> false;
            default -> throw new IllegalArgumentException(
                    "Cannot instantiate ForkJoinExecutorServiceFactory. "
                            + "\"task-peeking-mode\" in \"fork-join-executor\" section could only set to \"FIFO\" or \"LIFO\".");
        };
        this.maximumPoolSize = config.getInt("maximum-pool-size");
        this.minimumRunnable = resolveMinimumRunnable(config.getInt("minimum-runnable"), parallelism,
                Runtime.version().feature());
    }

    @Override
    public boolean isVirtualized() {
        return isVirtualized;
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        ThreadFactory namedFactory = threadFactory instanceof MonitorableThreadFactory monitorable
                ? monitorable.withName(monitorable.name() + "-" + id)
                : threadFactory;
        if (!(namedFactory instanceof ForkJoinPool.ForkJoinWorkerThreadFactory workerFactory)) {
            throw new IllegalStateException(
                    "The prerequisites for the ForkJoinExecutorConfigurator is a ForkJoinPool.ForkJoinWorkerThreadFactory!");
        }
        if (isVirtualized) {
            String prefix = namedFactory instanceof MonitorableThreadFactory monitorable
                    ? monitorable.name()
                    : id;
            return () -> VirtualThreadSupport.createVirtualized(namedFactory,
                    new TapioForkJoinPool(parallelism, workerFactory, maximumPoolSize, asyncMode, minimumRunnable),
                    prefix, virtualThreadStartNumber);
        }
        return () -> new TapioForkJoinPool(parallelism, workerFactory, maximumPoolSize, asyncMode, minimumRunnable);
    }

    private static final class TapioForkJoinPool extends ForkJoinPool implements LoadMetrics {
        private TapioForkJoinPool(int parallelism, ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory,
                                  int maximumPoolSize, boolean asyncMode, int minimumRunnable) {
            super(parallelism, threadFactory, MonitorableThreadFactory.doNothing, asyncMode, 0, maximumPoolSize,
                    minimumRunnable, null, ForkJoinPoolConstants.DefaultKeepAliveMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void execute(Runnable runnable) {
            if (runnable == null) throw new NullPointerException("Runnable was null");
            super.execute(runnable instanceof ForkJoinTask<?> task ? task : new TapioForkJoinTask(runnable));
        }

        @Override
        public boolean atFullThrottle() {
            return getActiveThreadCount() >= getParallelism();
        }
    }

    private static final class TapioForkJoinTask extends ForkJoinTask<Void> {
        private final Runnable runnable;

        private TapioForkJoinTask(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public Void getRawResult() {
            return null;
        }

        @Override
        protected void setRawResult(Void value) {
        }

        @Override
        protected boolean exec() {
            try {
                runnable.run();
                return true;
            } catch (Throwable error) {
                if (error instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                Thread.UncaughtExceptionHandler handler = Thread.currentThread().getUncaughtExceptionHandler();
                if (handler != null) handler.uncaughtException(Thread.currentThread(), error);
                throw error;
            }
        }
    }
}
