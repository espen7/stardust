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

/**
 * 管理多个资源对应的 {@link FailureDetector} 实例的注册表契约。
 *
 * @param <A> 被监控资源的键类型
 */
public interface FailureDetectorRegistry<A> {

    /**
     * 判断指定资源当前是否可用；尚未监控的资源视为可用。
     *
     * @param resource 资源键
     * @return 可用时返回 {@code true}
     */
    boolean isAvailable(A resource);

    /**
     * 判断指定资源是否已开始监控。
     *
     * @param resource 资源键
     * @return 已开始监控时返回 {@code true}
     */
    boolean isMonitoring(A resource);

    /**
     * 为指定资源记录一次心跳。
     *
     * @param resource 资源键
     */
    void heartbeat(A resource);

    /**
     * 移除指定资源的失效检测器。
     *
     * @param resource 资源键
     */
    void remove(A resource);

    /**
     * 清空所有已注册的失效检测器。
     */
    void reset();
}
