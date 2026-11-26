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

import io.github.espen7.tapio.annotation.InternalApi;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * 封装 Java 虚拟线程的创建与执行器支持，对应 Pekko {@code VirtualThreadSupport}。
 */
@InternalApi
public final class VirtualThreadSupport {
    /** 当前 JVM 是否支持虚拟线程（Java 21+ 恒为 {@code true}）。 */
    public static final boolean isSupported = Runtime.version().feature() >= 21;

    private static final MethodHandles.Lookup PRIVATE_LOOKUP = MethodHandles.lookup();
    private static final ClassValue<VarHandle> SCHEDULER_HANDLES = new ClassValue<>() {
        @Override
        protected VarHandle computeValue(Class<?> clazz) {
            try {
                return MethodHandles.privateLookupIn(clazz, PRIVATE_LOOKUP)
                        .findVarHandle(clazz, "scheduler", Executor.class);
            } catch (ReflectiveOperationException error) {
                return null;
            }
        }
    };

    private VirtualThreadSupport() {
    }

    /**
     * 使用指定线程工厂创建每个任务一个线程的执行器。
     *
     * @param threadFactory 线程工厂
     * @return ExecutorService 实例
     */
    public static ExecutorService newThreadPerTaskExecutor(ThreadFactory threadFactory) {
        Objects.requireNonNull(threadFactory, "threadFactory should not be null.");
        return Executors.newThreadPerTaskExecutor(threadFactory);
    }

    /**
     * 使用默认虚拟线程调度器启动一个虚拟线程。
     *
     * @param prefix 线程名称前缀
     * @param runnable 待执行任务
     * @return 已启动的虚拟线程
     */
    public static Thread startVirtualThread(String prefix, Runnable runnable) {
        Objects.requireNonNull(runnable, "runnable should not be null.");
        Thread thread = newVirtualThreadFactory(prefix, -1).newThread(runnable);
        thread.start();
        return thread;
    }

    /**
     * 使用默认虚拟线程调度器创建虚拟线程工厂。
     *
     * @param prefix 线程名称前缀
     * @param start 起始编号；若 {@code <= -1} 则不追加编号
     * @return 虚拟线程工厂
     */
    public static ThreadFactory newVirtualThreadFactory(String prefix, int start) {
        return newVirtualThreadFactory(prefix, start, null);
    }

    /**
     * 创建虚拟线程工厂，并在 JVM 允许访问内部 {@code scheduler} 字段时绑定自定义调度器。
     *
     * @param prefix 线程名称前缀
     * @param start 起始编号；若 {@code <= -1} 则不追加编号
     * @param executor 自定义调度器；为 {@code null} 时使用默认调度器
     * @return 虚拟线程工厂
     */
    public static ThreadFactory newVirtualThreadFactory(String prefix, int start, ExecutorService executor) {
        if (!isSupported) {
            throw new IllegalArgumentException("Virtual thread is not supported.");
        }
        if (prefix == null || prefix.isEmpty()) {
            throw new IllegalArgumentException("prefix should not be null or empty.");
        }
        Thread.Builder.OfVirtual builder = Thread.ofVirtual();
        if (start <= -1) {
            builder = builder.name(prefix + "-virtual-thread");
        } else {
            builder = builder.name(prefix + "-virtual-thread-", start);
        }
        if (executor != null) {
            VarHandle schedulerHandle = SCHEDULER_HANDLES.get(builder.getClass());
            if (schedulerHandle != null) {
                schedulerHandle.set(builder, executor);
            }
        }
        return builder.factory();
    }

    static ExecutorService createVirtualized(ThreadFactory threadFactory, ExecutorService pool,
            String prefixName, int startNumber) {
        ThreadFactory factory;
        if (threadFactory instanceof MonitorableThreadFactory monitorable) {
            ThreadFactory vtFactory = newVirtualThreadFactory(monitorable.name(), startNumber, pool);
            factory = runnable -> {
                Thread vt = vtFactory.newThread(runnable);
                vt.setUncaughtExceptionHandler(monitorable.exceptionHandler());
                if (monitorable.contextClassLoader() != null) {
                    vt.setContextClassLoader(monitorable.contextClassLoader());
                }
                return vt;
            };
        } else {
            factory = newVirtualThreadFactory(prefixName, startNumber, pool);
        }
        return new VirtualizedExecutorService(factory, pool, true);
    }
}
