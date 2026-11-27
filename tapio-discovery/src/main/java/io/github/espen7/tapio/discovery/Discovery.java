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
package io.github.espen7.tapio.discovery;

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务发现扩展入口，依据 {@code tapio.discovery.method} 加载并缓存 {@link ServiceDiscovery} 实现。
 */
public final class Discovery implements Extension {

    /** 扩展 ID。 */
    public static final AbstractExtensionId<Discovery> Id = new AbstractExtensionId<>() {
        @Override
        public Discovery createExtension(ExtendedActorSystem system) {
            return new Discovery(system);
        }
    };

    /**
     * 获取或创建关联的 {@link Discovery} 扩展实例。
     */
    public static Discovery get(ActorSystem system) {
        return Id.apply(system);
    }

    private final ExtendedActorSystem system;
    private final ConcurrentHashMap<String, ServiceDiscovery> implementations = new ConcurrentHashMap<>();
    private volatile ServiceDiscovery defaultImpl;

    private Discovery(ExtendedActorSystem system) {
        this.system = system;
    }

    /**
     * 返回默认配置 {@code tapio.discovery.method} 对应的 {@link ServiceDiscovery}。
     */
    public ServiceDiscovery discovery() {
        ServiceDiscovery d = defaultImpl;
        if (d == null) {
            String method = system.settings().config().getString("tapio.discovery.method");
            if ("<method>".equals(method) || method.isBlank()) {
                throw new IllegalArgumentException(
                        "No default service discovery implementation configured in `tapio.discovery.method`. "
                                + "Configure e.g. 'config' or 'kubernetes-api'.");
            }
            d = loadServiceDiscovery(method);
            defaultImpl = d;
        }
        return d;
    }

    /**
     * 根据方法名或完整类名加载并缓存 {@link ServiceDiscovery} 实例。
     *
     * @param method 配置路径名（如 {@code "config"}、{@code "kubernetes-api"}）或完整类名
     * @return {@link ServiceDiscovery} 实例
     */
    public ServiceDiscovery loadServiceDiscovery(String method) {
        return implementations.computeIfAbsent(method, this::createServiceDiscovery);
    }

    @InternalApi
    private ServiceDiscovery createServiceDiscovery(String method) {
        Config config = system.settings().config();
        String className = resolveClassName(config, method);
        try {
            return system.dynamicAccess().createInstanceFor(
                    className,
                    ServiceDiscovery.class,
                    List.of(Map.entry(ExtendedActorSystem.class, system)));
        } catch (Throwable first) {
            try {
                return system.dynamicAccess().createInstanceFor(
                        className,
                        ServiceDiscovery.class,
                        List.of(Map.entry(ActorSystem.class, system)));
            } catch (Throwable second) {
                try {
                    return system.dynamicAccess().createInstanceFor(
                            className,
                            ServiceDiscovery.class,
                            List.of());
                } catch (Throwable third) {
                    if (third instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new IllegalStateException(
                            "Failed to instantiate ServiceDiscovery [" + className + "] for method [" + method + "]",
                            third);
                }
            }
        }
    }

    private static String resolveClassName(Config config, String method) {
        String discoveryPath = "tapio.discovery." + method + ".class";
        if (config.hasPath(discoveryPath)) {
            return config.getString(discoveryPath);
        }
        String rootPath = method + ".class";
        if (config.hasPath(rootPath)) {
            return config.getString(rootPath);
        }
        return method;
    }
}
