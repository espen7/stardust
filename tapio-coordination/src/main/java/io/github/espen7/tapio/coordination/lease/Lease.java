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
package io.github.espen7.tapio.coordination.lease;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * 分布式互斥租约（Lease）抽象基类，由第三方协调后端实现。
 */
public abstract class Lease {

    private final LeaseSettings settings;

    protected Lease(LeaseSettings settings) {
        this.settings = settings;
    }

    /** 返回关联的 {@link LeaseSettings}。 */
    public final LeaseSettings getSettings() {
        return settings;
    }

    /**
     * 尝试获取租约；成功返回 {@code true}，已被其他持有者占用返回 {@code false}。
     */
    public abstract CompletionStage<Boolean> acquire();

    /**
     * 尝试获取租约，并注册租约意外丢失时的回调。
     */
    public abstract CompletionStage<Boolean> acquire(Consumer<Optional<Throwable>> leaseLostCallback);

    /**
     * 释放租约，以便其他持有者可以获取。
     */
    public abstract CompletionStage<Boolean> release();

    /**
     * 检查当前持有者是否仍确定持有该租约。
     */
    public abstract boolean checkLease();
}
