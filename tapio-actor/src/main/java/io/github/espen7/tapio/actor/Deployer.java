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
package io.github.espen7.tapio.actor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.github.espen7.tapio.util.WildcardIndex;

import java.util.Optional;

/**
 * 将 ActorPath 映射到部署配置
 * {@code actor/src/main/scala/org/apache/pekko/actor/Deployer.scala} 的 {@code Deployer}。
 *
 * <p>只解析 {@code tapio.actor.deployment} 下的 {@code dispatcher} 和 {@code mailbox}。
 * 配置键按去掉 guardian 段后的路径匹配，例如顶层 Actor 的 {@code /user/foo} 对应
 * 配置键 {@code /foo}。</p>
 */
public final class Deployer {
    private final Config config;
    private final Config defaultDeployment;
    private final WildcardIndex<Deploy> deployments;

    /**
     * 从系统配置创建 Deployer。
     *
     * @param settings 所属系统的有效配置
     */
    public Deployer(ActorSystem.Settings settings) {
        this.config = settings.config().getConfig(ActorPath.Scheme + ".actor.deployment");
        this.defaultDeployment = config.getConfig("default");
        WildcardIndex<Deploy> index = new WildcardIndex<>();
        for (var entry : config.root().entrySet()) {
            if ("default".equals(entry.getKey())) continue;
            if (!(entry.getValue() instanceof ConfigObject object)) continue;
            Optional<Deploy> deploy = parseConfig(entry.getKey(), object.toConfig());
            if (deploy.isPresent()) index = index.insert(elements(entry.getKey()), deploy.get());
        }
        this.deployments = index;
    }

    /**
     * 查找与 ActorPath 匹配的部署配置。
     *
     * @param path 待查找的路径
     * @return 匹配的部署配置；没有匹配时为 {@link Optional#empty()}
     */
    public Optional<Deploy> lookup(ActorPath path) {
        return deployments.find(pathElements(path));
    }

    private Optional<Deploy> parseConfig(String key, Config section) {
        Config deployment = section.withFallback(defaultDeployment);
        String dispatcher = deployment.getString("dispatcher");
        String mailbox = deployment.getString("mailbox");
        return Optional.of(new Deploy(key, dispatcher, mailbox));
    }

    private static Iterable<String> pathElements(ActorPath path) {
        String[] elements = path.toStringWithoutAddress().split("/");
        java.util.List<String> result = new java.util.ArrayList<>(elements.length);
        for (int i = 0; i < elements.length; i++) {
            if (elements[i].isEmpty()) continue;
            result.add(elements[i]);
        }
        if (!result.isEmpty()) result.remove(0);
        return result;
    }

    private static String[] elements(String key) {
        String[] parts = key.split("/");
        return java.util.Arrays.copyOfRange(parts, 1, parts.length);
    }
}
