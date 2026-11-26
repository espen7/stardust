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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/** 为普通线程和 ForkJoin worker 设置统一名称、daemon、上下文类加载器与异常处理器。 */
public final class MonitorableThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory {
    static final Thread.UncaughtExceptionHandler doNothing = (thread, cause) -> {};

    private final String name;
    private final boolean daemonic;
    private final ClassLoader contextClassLoader;
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final AtomicLong counter;

    /**
     * 创建线程工厂。
     *
     * @param name 线程名前缀
     * @param daemonic 是否创建 daemon 线程
     * @param contextClassLoader 线程上下文类加载器，可以为 {@code null}
     * @param exceptionHandler 未捕获异常处理器，可以为 {@code null}
     */
    public MonitorableThreadFactory(String name, boolean daemonic, ClassLoader contextClassLoader,
                                    Thread.UncaughtExceptionHandler exceptionHandler) {
        this(name, daemonic, contextClassLoader, exceptionHandler, new AtomicLong());
    }

    private MonitorableThreadFactory(String name, boolean daemonic, ClassLoader contextClassLoader,
                                     Thread.UncaughtExceptionHandler exceptionHandler, AtomicLong counter) {
        this.name = name;
        this.daemonic = daemonic;
        this.contextClassLoader = contextClassLoader;
        this.exceptionHandler = exceptionHandler;
        this.counter = counter;
    }

    /** 返回当前线程名前缀。 */
    public String name() {
        return name;
    }

    ClassLoader contextClassLoader() {
        return contextClassLoader;
    }

    Thread.UncaughtExceptionHandler exceptionHandler() {
        return exceptionHandler;
    }

    /**
     * 返回使用新前缀但共享线程计数器和线程属性的工厂。
     *
     * @param newName 新的线程名前缀
     * @return 派生的线程工厂
     */
    public MonitorableThreadFactory withName(String newName) {
        return new MonitorableThreadFactory(newName, daemonic, contextClassLoader, exceptionHandler, counter);
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return wire(new MonitorableForkJoinWorkerThread(pool));
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return wire(new Thread(runnable));
    }

    private <T extends Thread> T wire(T thread) {
        thread.setName(name + "-" + counter.incrementAndGet());
        if (exceptionHandler != null) thread.setUncaughtExceptionHandler(exceptionHandler);
        thread.setDaemon(daemonic);
        if (contextClassLoader != null) thread.setContextClassLoader(contextClassLoader);
        return thread;
    }

    private static final class MonitorableForkJoinWorkerThread extends ForkJoinWorkerThread {
        private MonitorableForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
        }
    }
}
