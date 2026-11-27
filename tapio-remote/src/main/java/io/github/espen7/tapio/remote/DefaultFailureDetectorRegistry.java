/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.annotation.InternalApi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 无锁读、原子写更新的线程安全 {@link FailureDetectorRegistry} 实现。
 *
 * @param <A> 被监控资源的键类型
 */
public class DefaultFailureDetectorRegistry<A> implements FailureDetectorRegistry<A> {
    private final Supplier<FailureDetector> detectorFactory;
    private final AtomicReference<Map<A, FailureDetector>> resourceToFailureDetector =
            new AtomicReference<>(Collections.emptyMap());
    private final Lock failureDetectorCreationLock = new ReentrantLock();

    /**
     * 创建失效检测器注册表。
     *
     * @param detectorFactory 创建新 {@link FailureDetector} 实例的工厂函数
     */
    public DefaultFailureDetectorRegistry(Supplier<FailureDetector> detectorFactory) {
        this.detectorFactory = detectorFactory;
    }

    @Override
    public final boolean isAvailable(A resource) {
        FailureDetector fd = resourceToFailureDetector.get().get(resource);
        return fd == null || fd.isAvailable();
    }

    @Override
    public final boolean isMonitoring(A resource) {
        FailureDetector fd = resourceToFailureDetector.get().get(resource);
        return fd != null && fd.isMonitoring();
    }

    @Override
    public final void heartbeat(A resource) {
        FailureDetector existing = resourceToFailureDetector.get().get(resource);
        if (existing != null) {
            existing.heartbeat();
            return;
        }
        failureDetectorCreationLock.lock();
        try {
            Map<A, FailureDetector> oldTable = resourceToFailureDetector.get();
            FailureDetector fd = oldTable.get(resource);
            if (fd != null) {
                fd.heartbeat();
            } else {
                FailureDetector newDetector = detectorFactory.get();
                if (newDetector instanceof FailureDetectorWithAddress dwa) {
                    dwa.setAddress(String.valueOf(resource));
                }
                newDetector.heartbeat();
                Map<A, FailureDetector> newTable = new HashMap<>(oldTable);
                newTable.put(resource, newDetector);
                resourceToFailureDetector.set(Collections.unmodifiableMap(newTable));
            }
        } finally {
            failureDetectorCreationLock.unlock();
        }
    }

    @Override
    public final void remove(A resource) {
        while (true) {
            Map<A, FailureDetector> oldTable = resourceToFailureDetector.get();
            if (!oldTable.containsKey(resource)) {
                return;
            }
            Map<A, FailureDetector> newTable = new HashMap<>(oldTable);
            newTable.remove(resource);
            if (resourceToFailureDetector.compareAndSet(oldTable, Collections.unmodifiableMap(newTable))) {
                return;
            }
        }
    }

    @Override
    public final void reset() {
        while (true) {
            Map<A, FailureDetector> oldTable = resourceToFailureDetector.get();
            if (resourceToFailureDetector.compareAndSet(oldTable, Collections.emptyMap())) {
                return;
            }
        }
    }

    /**
     * INTERNAL API：获取指定资源对应的底层 {@link FailureDetector}。
     *
     * @param resource 资源键
     * @return 对应的失效检测器（若存在）
     */
    @InternalApi
    public Optional<FailureDetector> failureDetector(A resource) {
        return Optional.ofNullable(resourceToFailureDetector.get().get(resource));
    }
}
