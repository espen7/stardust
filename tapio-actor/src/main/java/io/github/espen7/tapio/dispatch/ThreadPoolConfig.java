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

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Supplier;

/** ThreadPoolExecutor 的不可变有效配置。 */
record ThreadPoolConfig(boolean allowCorePoolTimeout, int corePoolSize, int maxPoolSize,
                        Duration threadTimeout, Supplier<BlockingQueue<Runnable>> queueFactory,
                        RejectedExecutionHandler rejectionPolicy) {
    static final boolean defaultAllowCoreThreadTimeout = false;
    static final int defaultCorePoolSize = 16;
    static final int defaultMaxPoolSize = 128;
    static final Duration defaultTimeout = Duration.ofMillis(60000L);
    static final RejectedExecutionHandler defaultRejectionPolicy = new SaneRejectedExecutionHandler();

    static ThreadPoolConfig defaults() {
        return new ThreadPoolConfig(defaultAllowCoreThreadTimeout, defaultCorePoolSize, defaultMaxPoolSize,
                defaultTimeout, linkedBlockingQueue(), defaultRejectionPolicy);
    }

    ThreadPoolConfig withPoolSize(int corePoolSize, int maxPoolSize) {
        return new ThreadPoolConfig(allowCorePoolTimeout, corePoolSize, maxPoolSize, threadTimeout, queueFactory,
                rejectionPolicy);
    }

    static int scaledPoolSize(int floor, double multiplier, int ceiling) {
        return Math.min(Math.max((int) Math.ceil(Runtime.getRuntime().availableProcessors() * multiplier), floor), ceiling);
    }

    static Supplier<BlockingQueue<Runnable>> arrayBlockingQueue(int capacity, boolean fair) {
        return () -> new ArrayBlockingQueue<>(capacity, fair);
    }

    static Supplier<BlockingQueue<Runnable>> synchronousQueue(boolean fair) {
        return () -> new SynchronousQueue<>(fair);
    }

    static Supplier<BlockingQueue<Runnable>> linkedBlockingQueue() {
        return LinkedBlockingQueue::new;
    }

    static Supplier<BlockingQueue<Runnable>> linkedBlockingQueue(int capacity) {
        return () -> new LinkedBlockingQueue<>(capacity);
    }
}
