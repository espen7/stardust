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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** 将 ExecutorService 调用委托给延迟创建的实际 executor。 */
public interface ExecutorServiceDelegate extends ExecutorService {
    /**
     * 返回当前实际的 ExecutorService。
     *
     * @return 实际 executor
     */
    ExecutorService executor();

    @Override
    default void execute(Runnable command) {
        executor().execute(command);
    }

    @Override
    default void shutdown() {
        executor().shutdown();
    }

    @Override
    default java.util.List<Runnable> shutdownNow() {
        return executor().shutdownNow();
    }

    @Override
    default boolean isShutdown() {
        return executor().isShutdown();
    }

    @Override
    default boolean isTerminated() {
        return executor().isTerminated();
    }

    @Override
    default boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor().awaitTermination(timeout, unit);
    }

    default <T> java.util.concurrent.Future<T> submit(Callable<T> task) {
        return executor().submit(task);
    }

    default <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
        return executor().submit(task, result);
    }

    default java.util.concurrent.Future<?> submit(Runnable task) {
        return executor().submit(task);
    }

    default <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor().invokeAll(tasks);
    }

    default <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor().invokeAll(tasks, timeout, unit);
    }

    default <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        return executor().invokeAny(tasks);
    }

    default <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        return executor().invokeAny(tasks, timeout, unit);
    }
}
