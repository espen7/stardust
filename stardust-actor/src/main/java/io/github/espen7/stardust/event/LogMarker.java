package io.github.espen7.stardust.event;

import java.util.Map;
import java.util.Objects;

/** 结构化日志事件使用的 marker 名称和属性。 */
public class LogMarker {
    /** marker 属性中保存消息 class 的键。 */
    public static final String MESSAGE_CLASS = "pekkoMessageClass";

    private final String name;
    private final Map<String, Object> properties;

    /** 创建不带属性的 marker。 */
    public LogMarker(String name) {
        this(name, Map.of());
    }

    /** 创建带属性的 marker。 */
    public LogMarker(String name, Map<String, Object> properties) {
        this.name = Objects.requireNonNull(name, "name");
        this.properties = Map.copyOf(properties);
    }

    public String name() {
        return name;
    }

    public Map<String, Object> properties() {
        return properties;
    }

    /** Java API：返回 marker 属性。 */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /** Java API 的 marker 工厂。 */
    public static LogMarker create(String name) {
        return new LogMarker(name);
    }

    /** Java API 的 marker 工厂。 */
    public static LogMarker create(String name, Map<String, Object> properties) {
        return new LogMarker(name, properties);
    }
}
