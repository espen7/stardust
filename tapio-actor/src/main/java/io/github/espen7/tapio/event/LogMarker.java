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
package io.github.espen7.tapio.event;

import java.util.Map;
import java.util.Objects;

/** 结构化日志事件使用的 marker 名称和属性。 */
public class LogMarker {
    /** marker 属性中保存消息 class 的键。 */
    public static final String MESSAGE_CLASS = "tapioMessageClass";

    private final String name;
    private final Map<String, Object> properties;

    /**
     * 创建不带属性的 marker。
     *
     * @param name marker 名称
     */
    public LogMarker(String name) {
        this(name, Map.of());
    }

    /**
     * 创建带属性的 marker。
     *
     * @param name marker 名称
     * @param properties marker 属性
     */
    public LogMarker(String name, Map<String, Object> properties) {
        this.name = Objects.requireNonNull(name, "name");
        this.properties = Map.copyOf(properties);
    }

    /**
     * 返回 marker 名称。
     *
     * @return marker 名称
     */
    public String name() {
        return name;
    }

    /**
     * 返回 marker 属性。
     *
     * @return marker 属性
     */
    public Map<String, Object> properties() {
        return properties;
    }

    /**
     * Java API：返回 marker 属性。
     *
     * @return marker 属性
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Java API 的 marker 工厂。
     *
     * @param name marker 名称
     * @return 新 marker
     */
    public static LogMarker create(String name) {
        return new LogMarker(name);
    }

    /**
     * Java API 的 marker 工厂。
     *
     * @param name marker 名称
     * @param properties marker 属性
     * @return 新 marker
     */
    public static LogMarker create(String name, Map<String, Object> properties) {
        return new LogMarker(name, properties);
    }
}
