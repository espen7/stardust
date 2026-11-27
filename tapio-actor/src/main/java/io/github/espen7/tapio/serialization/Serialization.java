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
package io.github.espen7.tapio.serialization;

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.ActorRefWithCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ConfigKeys;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.util.NonFatal;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 序列化模块扩展，负责根据配置与编程式注册定位 {@link Serializer} 并执行序列化与反序列化。
 */
public class Serialization implements Extension {
    private static final String TAPIO_PACKAGE_PREFIX = "io.github.espen7.tapio.";

    /**
     * INTERNAL API：保存当前线程用于序列化本地 {@link ActorRef} 的传输上下文信息。
     */
    @InternalApi
    public static final ThreadLocal<Information> currentTransportInformation = new ThreadLocal<>();

    /**
     * 序列化子系统配置视图。
     */
    public static final class Settings {
        private final Config config;
        private final Map<String, String> serializers;
        private final Map<String, String> serializationBindings;

        /**
         * 从配置构造序列化配置视图。
         *
         * @param config 系统配置
         */
        public Settings(Config config) {
            this.config = config;
            this.serializers = configToMap(config.getConfig(ConfigKeys.ACTOR_SERIALIZERS));
            this.serializationBindings = configToMap(config.getConfig(ConfigKeys.ACTOR_SERIALIZATION_BINDINGS));
        }

        /**
         * 返回底层配置。
         *
         * @return Typesafe Config
         */
        public Config config() {
            return config;
        }

        /**
         * 返回别名到序列化器类名的映射。
         *
         * @return 不可变映射
         */
        public Map<String, String> serializers() {
            return serializers;
        }

        /**
         * 返回类名到序列化器别名的绑定映射。
         *
         * @return 不可变映射
         */
        public Map<String, String> serializationBindings() {
            return serializationBindings;
        }

        private static Map<String, String> configToMap(Config cfg) {
            Map<String, String> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : cfg.root().unwrapped().entrySet()) {
                result.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return Collections.unmodifiableMap(result);
        }
    }

    /**
     * 保存序列化时的传输地址与关联系统。
     *
     * @param address ActorSystem 的传输地址
     * @param system 关联的 {@link ActorSystem}
     */
    public record Information(Address address, ActorSystem system) {
    }

    /**
     * 类与对应序列化器的绑定条目。
     *
     * @param clazz 绑定的目标类或接口
     * @param serializer 对应的序列化器
     */
    public record ClassSerializer(Class<?> clazz, Serializer serializer) {
    }

    /**
     * 计算 {@link ActorRef} 的序列化路径字符串，优先使用当前线程绑定的 {@link Information}。
     *
     * @param actorRef 目标 ActorRef
     * @return 序列化格式的路径字符串
     */
    public static String serializedActorPath(ActorRef actorRef) {
        ActorPath path = actorRef.path();
        ExtendedActorSystem originalSystem = actorRef instanceof ActorRefWithCell refWithCell
                ? (ExtendedActorSystem) refWithCell.underlying().system()
                : null;
        Information info = currentTransportInformation.get();
        if (info == null) {
            if (originalSystem == null) {
                return path.toSerializationFormat();
            }
            try {
                return path.toSerializationFormatWithAddress(originalSystem.provider().getDefaultAddress());
            } catch (Throwable t) {
                if (!NonFatal.apply(t)) throw t;
                return path.toSerializationFormat();
            }
        }
        if (originalSystem == null || originalSystem.equals(info.system())) {
            return path.toSerializationFormatWithAddress(info.address());
        }
        ActorRefProvider provider = originalSystem.provider();
        Address external = provider.getExternalAddressFor(info.address()).orElseGet(provider::getDefaultAddress);
        return path.toSerializationFormatWithAddress(external);
    }

    /**
     * 在当前线程设置传输序列化信息后执行 {@code supplier}。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     * @param supplier 执行逻辑
     * @param <T> 返回类型
     * @return 执行结果
     */
    public static <T> T withTransportInformation(ExtendedActorSystem system, Supplier<T> supplier) {
        Information info = system.provider().serializationInformation();
        Information prev = currentTransportInformation.get();
        if (prev == info) {
            return supplier.get();
        }
        currentTransportInformation.set(info);
        try {
            return supplier.get();
        } finally {
            if (prev == null) {
                currentTransportInformation.remove();
            } else {
                currentTransportInformation.set(prev);
            }
        }
    }

    /**
     * 返回当前线程绑定的传输序列化信息。
     *
     * @return 当前 {@link Information}
     * @throws IllegalStateException 当前线程未绑定传输信息时抛出
     */
    public static Information getCurrentTransportInformation() {
        Information info = currentTransportInformation.get();
        if (info == null) {
            throw new IllegalStateException(
                    "currentTransportInformation is not set, use Serialization.withTransportInformation");
        }
        return info;
    }

    private final ExtendedActorSystem system;
    private final Settings settings;
    private final boolean allowJavaSerialization;
    private final LoggingAdapter log;
    private final AtomicReference<Map<String, Optional<Class<?>>>> manifestCache =
            new AtomicReference<>(Map.of());
    private final int maxNestingDepth;
    private final List<SerializerDetails> serializerDetails;
    private final Map<String, Serializer> serializers;
    private final List<ClassSerializer> bindings;
    private final ConcurrentHashMap<Class<?>, Serializer> serializerMap = new ConcurrentHashMap<>();
    private final Map<Integer, Serializer> serializerByIdentity;
    private final Serializer[] quickSerializerByIdentity;
    private final boolean isJavaSerializationWarningEnabled;
    private final boolean isWarningOnNoVerificationEnabled;

    /**
     * 为指定 {@link ExtendedActorSystem} 初始化序列化扩展。
     *
     * @param system 所属系统
     */
    public Serialization(ExtendedActorSystem system) {
        this.system = Objects.requireNonNull(system, "system");
        this.settings = new Settings(system.settings().config());
        this.allowJavaSerialization = system.settings().AllowJavaSerialization;
        this.log = Logging.getLogger(system, this, getClass());
        this.maxNestingDepth = system.settings().SerializationMaxNestingDepth;
        this.isJavaSerializationWarningEnabled = system.settings().WarnAboutJavaSerializerUsage;
        this.isWarningOnNoVerificationEnabled = system.settings().WarnOnNoSerializationVerification;

        Optional<SerializationSetup> setupOpt = system.settings().setup.get(SerializationSetup.class);
        List<SerializerDetails> rawDetails = setupOpt.map(s -> s.createSerializers(system)).orElse(List.of());
        List<SerializerDetails> normalizedDetails = new ArrayList<>(rawDetails.size());
        for (SerializerDetails det : rawDetails) {
            if (isDisallowedJavaSerializer(det.serializer())) {
                log.debug(
                        "Replacing JavaSerializer with DisabledJavaSerializer, due to `tapio.actor.allow-java-serialization = off`.");
                normalizedDetails.add(SerializerDetails.create(
                        det.alias(), new DisabledJavaSerializer(system), det.useFor()));
            } else {
                normalizedDetails.add(det);
            }
        }
        this.serializerDetails = List.copyOf(normalizedDetails);

        Map<String, Serializer> serMap = new HashMap<>();
        for (Map.Entry<String, String> entry : settings.serializers().entrySet()) {
            serMap.put(entry.getKey(), serializerOf(entry.getKey(), entry.getValue()));
        }
        for (SerializerDetails det : serializerDetails) {
            serMap.put(det.alias(), det.serializer());
        }
        ensureOnlyAllowedSerializers(serMap.values());
        this.serializers = Collections.unmodifiableMap(serMap);

        List<ClassSerializer> rawBindings = new ArrayList<>();
        for (Map.Entry<String, String> entry : settings.serializationBindings().entrySet()) {
            String className = entry.getKey();
            String alias = entry.getValue();
            if (!"none".equals(alias) && checkGoogleProtobuf(className) && checkScalaPb(className)) {
                Class<?> boundClass = loadBindingClass(className);
                Serializer boundSer = serializers.get(alias);
                if (boundSer == null) {
                    throw new ConfigurationException(
                            "Serialization binding [" + className + "] references unknown serializer alias [" + alias + "]");
                }
                rawBindings.add(new ClassSerializer(boundClass, boundSer));
            }
        }
        for (SerializerDetails det : serializerDetails) {
            for (Class<?> clazz : det.useFor()) {
                rawBindings.add(new ClassSerializer(clazz, det.serializer()));
            }
        }
        this.bindings = sort(rawBindings);
        ensureOnlyAllowedSerializers(this.bindings.stream().map(ClassSerializer::serializer).toList());
        for (ClassSerializer cs : this.bindings) {
            warnUnexpectedNonTapioSerializer(cs.clazz(), cs.serializer());
            this.serializerMap.put(cs.clazz(), cs.serializer());
        }

        Map<Integer, Serializer> idMap = new HashMap<>();
        idMap.put(NullSerializer.INSTANCE.identifier(), NullSerializer.INSTANCE);
        for (Serializer ser : this.serializers.values()) {
            int id = ser.identifier();
            Serializer existing = idMap.get(id);
            if (existing != null && existing != ser) {
                throw new IllegalArgumentException(
                        "Serializer identifier [" + id + "] of [" + ser.getClass().getName()
                                + "] is not unique. It is also used by [" + existing.getClass().getName() + "].");
            }
            idMap.put(id, ser);
        }
        this.serializerByIdentity = Collections.unmodifiableMap(idMap);

        Serializer[] table = new Serializer[1024];
        for (Map.Entry<Integer, Serializer> entry : this.serializerByIdentity.entrySet()) {
            int id = entry.getKey();
            if (0 <= id && id < table.length) {
                table[id] = entry.getValue();
            }
        }
        this.quickSerializerByIdentity = table;
    }

    /**
     * 返回关联的 {@link ExtendedActorSystem}。
     *
     * @return ActorSystem
     */
    public ExtendedActorSystem system() {
        return system;
    }

    /**
     * 返回序列化配置视图。
     *
     * @return {@link Settings}
     */
    public Settings settings() {
        return settings;
    }

    /**
     * 返回是否允许使用 Java 默认序列化。
     *
     * @return 允许时返回 {@code true}
     */
    public boolean allowJavaSerialization() {
        return allowJavaSerialization;
    }

    /**
     * INTERNAL API：返回当前系统的传输序列化信息。
     *
     * @return {@link Information}
     */
    @InternalApi
    public Information serializationInformation() {
        return system.provider().serializationInformation();
    }

    /**
     * 将对象序列化为字节数组。
     *
     * @param o 待序列化对象（允许为 {@code null}）
     * @return 序列化后的字节数组
     * @throws NotSerializableException 未找到序列化器或序列化失败时抛出
     */
    public byte[] serialize(Object o) throws NotSerializableException {
        Information prev = currentTransportInformation.get();
        if (prev == null) {
            currentTransportInformation.set(serializationInformation());
        }
        try {
            return findSerializerFor(o).toBinary(o);
        } finally {
            if (prev == null) {
                currentTransportInformation.remove();
            } else {
                currentTransportInformation.set(prev);
            }
        }
    }

    /**
     * 使用指定的序列化器 ID 与 manifest 从字节数组反序列化对象。
     *
     * @param bytes 字节数组
     * @param serializerId 序列化器唯一 ID
     * @param manifest manifest 类型提示
     * @return 反序列化后的对象
     * @throws NotSerializableException 未找到序列化器或反序列化失败时抛出
     */
    public Object deserialize(byte[] bytes, int serializerId, String manifest) throws NotSerializableException {
        Serializer serializer;
        try {
            serializer = getSerializerById(serializerId);
        } catch (NoSuchElementException e) {
            throw new NotSerializableException(
                    "Cannot find serializer with id [" + serializerId + "] (manifest [" + manifest
                            + "]). The most probable reason is that the configuration entry "
                            + ConfigKeys.ACTOR_SERIALIZERS + " is not in sync between the two systems.");
        }
        return deserializeByteArray(bytes, serializer, manifest == null ? "" : manifest);
    }

    /**
     * 使用指定类型查找序列化器并从字节数组反序列化对象。
     *
     * @param bytes 字节数组
     * @param clazz 目标类型
     * @param <T> 目标类型
     * @return 反序列化后的对象
     * @throws NotSerializableException 未找到序列化器或反序列化失败时抛出
     */
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws NotSerializableException {
        Information prev = currentTransportInformation.get();
        if (prev == null) {
            currentTransportInformation.set(serializationInformation());
        }
        try {
            return (T) serializerFor(clazz).fromBinary(bytes, clazz);
        } finally {
            if (prev == null) {
                currentTransportInformation.remove();
            } else {
                currentTransportInformation.set(prev);
            }
        }
    }

    /**
     * 使用指定的序列化器 ID 与 manifest 从 {@link ByteBuffer} 反序列化对象。
     *
     * @param buf 源缓冲区
     * @param serializerId 序列化器唯一 ID
     * @param manifest manifest 类型提示
     * @return 反序列化后的对象
     * @throws NotSerializableException 未找到序列化器或反序列化失败时抛出
     */
    public Object deserializeByteBuffer(ByteBuffer buf, int serializerId, String manifest)
            throws NotSerializableException {
        Serializer serializer;
        try {
            serializer = getSerializerById(serializerId);
        } catch (NoSuchElementException e) {
            throw new NotSerializableException(
                    "Cannot find serializer with id [" + serializerId + "] (manifest [" + manifest
                            + "]). The most probable reason is that the configuration entry "
                            + ConfigKeys.ACTOR_SERIALIZERS + " is not in synch between the two systems.");
        }
        String safeManifest = manifest == null ? "" : manifest;
        Information prev = currentTransportInformation.get();
        if (prev == null) {
            currentTransportInformation.set(serializationInformation());
        }
        try {
            if (serializer instanceof ByteBufferSerializer bbSer) {
                return bbSer.fromBinary(buf, safeManifest);
            }
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return deserializeByteArray(bytes, serializer, safeManifest);
        } finally {
            if (prev == null) {
                currentTransportInformation.remove();
            } else {
                currentTransportInformation.set(prev);
            }
        }
    }

    private Object deserializeByteArray(byte[] bytes, Serializer serializer, String manifest)
            throws NotSerializableException {
        Information prev = currentTransportInformation.get();
        if (prev == null) {
            currentTransportInformation.set(serializationInformation());
        }
        try {
            return NestedDeserialization.atNextLevel(maxNestingDepth, () -> {
                if (serializer instanceof SerializerWithStringManifest s2) {
                    return s2.fromBinary(bytes, manifest);
                }
                if (serializer instanceof NullSerializer && !manifest.isEmpty()) {
                    throw new NotSerializableException(
                            "Unexpected manifest [" + manifest + "] for serializer with id ["
                                    + serializer.identifier() + "].");
                }
                if (manifest.isEmpty() || !serializer.includeManifest()) {
                    return serializer.fromBinary(bytes, (Class<?>) null);
                }
                Map<String, Optional<Class<?>>> cache = manifestCache.get();
                Optional<Class<?>> cached = cache.get(manifest);
                if (cached != null) {
                    return serializer.fromBinary(bytes, cached.orElse(null));
                }
                try {
                    Class<?> loaded = system.dynamicAccess().getClassFor(manifest, Object.class);
                    Optional<Class<?>> opt = Optional.of(loaded);
                    updateManifestCache(manifest, opt);
                    return serializer.fromBinary(bytes, loaded);
                } catch (ClassNotFoundException e) {
                    throw new NotSerializableException(
                            "Cannot find manifest class [" + manifest + "] for serializer with id ["
                                    + serializer.identifier() + "].");
                }
            });
        } finally {
            if (prev == null) {
                currentTransportInformation.remove();
            } else {
                currentTransportInformation.set(prev);
            }
        }
    }

    private void updateManifestCache(String key, Optional<Class<?>> value) {
        while (true) {
            Map<String, Optional<Class<?>>> current = manifestCache.get();
            Map<String, Optional<Class<?>>> next = new HashMap<>(current);
            next.put(key, value);
            if (manifestCache.compareAndSet(current, Collections.unmodifiableMap(next))) {
                return;
            }
        }
    }

    /**
     * 查找给定对象的序列化器；若对象为 {@code null} 则返回 {@link NullSerializer#INSTANCE}。
     *
     * @param o 目标对象
     * @return 匹配的序列化器
     * @throws NotSerializableException 未配置对应绑定时抛出
     */
    public Serializer findSerializerFor(Object o) throws NotSerializableException {
        return o == null ? NullSerializer.INSTANCE : serializerFor(o.getClass());
    }

    /**
     * 按类层次查找最具体的序列化器绑定。
     *
     * @param clazz 目标类
     * @return 匹配的序列化器
     * @throws NotSerializableException 未配置对应绑定时抛出
     */
    public Serializer serializerFor(Class<?> clazz) throws NotSerializableException {
        Serializer cached = serializerMap.get(clazz);
        if (cached != null) {
            return cached;
        }
        List<ClassSerializer> possibilities = new ArrayList<>();
        for (ClassSerializer cs : bindings) {
            if (cs.clazz().isAssignableFrom(clazz)) {
                possibilities.add(cs);
            }
        }
        if (possibilities.isEmpty()) {
            throw new NotSerializableException("No configured serialization-bindings for class [" + clazz.getName() + "]");
        }
        Serializer resolved;
        if (isUniquePossibility(possibilities)) {
            resolved = possibilities.getFirst().serializer();
        } else {
            List<ClassSerializer> withoutJava = new ArrayList<>();
            for (ClassSerializer cs : possibilities) {
                Serializer s = cs.serializer();
                if (!(s instanceof JavaSerializer) && !(s instanceof DisabledJavaSerializer)) {
                    withoutJava.add(cs);
                }
            }
            if (withoutJava.isEmpty()) {
                throw new NotSerializableException(
                        "More than one JavaSerializer configured for class [" + clazz.getName() + "]");
            }
            if (!isUniquePossibility(withoutJava)) {
                String names = withoutJava.stream()
                        .map(cs -> cs.serializer().getClass().getName())
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("");
                log.warning("Multiple serializers found for [{}], choosing first of: [{}]", clazz.getName(), names);
            }
            resolved = withoutJava.getFirst().serializer();
        }

        Serializer existing = serializerMap.putIfAbsent(clazz, resolved);
        if (existing != null) {
            return existing;
        }
        if (shouldWarnAboutJavaSerializer(clazz, resolved)) {
            log.warning(
                    "Using the Java serializer for class [{}] which is not recommended because of performance implications. "
                            + "Use another serializer or disable this warning using the setting '"
                            + ConfigKeys.ACTOR_WARN_ABOUT_JAVA_SERIALIZER_USAGE + "'",
                    clazz.getName());
        }
        if (!warnUnexpectedNonTapioSerializer(clazz, resolved)) {
            log.debug("Using serializer [{}] for message [{}]", resolved.getClass().getName(), clazz.getName());
        }
        return resolved;
    }

    private static boolean isUniquePossibility(List<ClassSerializer> possibilities) {
        if (possibilities.size() == 1) {
            return true;
        }
        ClassSerializer head = possibilities.getFirst();
        boolean allAssignable = true;
        boolean allSameSerializer = true;
        for (ClassSerializer cs : possibilities) {
            if (!cs.clazz().isAssignableFrom(head.clazz())) {
                allAssignable = false;
            }
            if (!Objects.equals(cs.serializer(), head.serializer())) {
                allSameSerializer = false;
            }
        }
        return allAssignable || allSameSerializer;
    }

    /**
     * 通过全限定类名加载并实例化 {@link Serializer}。
     *
     * @param serializerFqn 序列化器全限定类名
     * @return 序列化器实例
     */
    public Serializer serializerOf(String serializerFqn) {
        return serializerOf("", serializerFqn);
    }

    private Serializer serializerOf(String bindingName, String serializerFqn) {
        String fqn = (!system.settings().AllowJavaSerialization && JavaSerializer.class.getName().equals(serializerFqn))
                ? DisabledJavaSerializer.class.getName()
                : serializerFqn;
        if (!fqn.equals(serializerFqn)) {
            log.debug(
                    "Replacing JavaSerializer with DisabledJavaSerializer, due to `tapio.actor.allow-java-serialization = off`.");
        }

        List<List<Map.Entry<Class<?>, Object>>> shapes = new ArrayList<>();
        shapes.add(List.of(Map.entry(ExtendedActorSystem.class, system)));
        shapes.add(List.of(Map.entry(ActorSystem.class, system)));
        shapes.add(List.of());
        if (!bindingName.isEmpty()) {
            shapes.add(List.of(Map.entry(ExtendedActorSystem.class, system), Map.entry(String.class, bindingName)));
            shapes.add(List.of(Map.entry(ActorSystem.class, system), Map.entry(String.class, bindingName)));
        }

        for (List<Map.Entry<Class<?>, Object>> args : shapes) {
            try {
                return system.dynamicAccess().createInstanceFor(fqn, Serializer.class, args);
            } catch (NoSuchMethodException ignored) {
                continue;
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new ConfigurationException("Cannot instantiate Serializer [" + fqn + "]", t);
            }
        }
        throw new ConfigurationException(
                "None of the supported constructors were found for serializer [" + fqn + "]. "
                        + "Supported constructor shapes (tried in order): "
                        + "(ExtendedActorSystem), (ActorSystem), (ClassicActorSystemProvider), (), "
                        + "(ExtendedActorSystem, String), (ActorSystem, String), (ClassicActorSystemProvider, String).");
    }

    /**
     * 按唯一 ID 查找序列化器。
     *
     * @param id 序列化器唯一 ID
     * @return 序列化器实例
     * @throws NoSuchElementException 未注册该 ID 时抛出
     */
    public Serializer getSerializerById(int id) {
        if (0 <= id && id < quickSerializerByIdentity.length) {
            Serializer ser = quickSerializerByIdentity[id];
            if (ser == null) {
                throw new NoSuchElementException("key not found: " + id);
            }
            return ser;
        }
        Serializer ser = serializerByIdentity.get(id);
        if (ser == null) {
            throw new NoSuchElementException("key not found: " + id);
        }
        return ser;
    }

    /**
     * 返回有序的序列化器绑定列表。
     *
     * @return 不可变绑定列表
     */
    @InternalApi
    public List<ClassSerializer> bindings() {
        return bindings;
    }

    /**
     * 返回唯一 ID 到序列化器的不可变映射。
     *
     * @return 唯一 ID 映射表
     */
    public Map<Integer, Serializer> serializerByIdentity() {
        return serializerByIdentity;
    }

    private Class<?> loadBindingClass(String className) {
        try {
            return system.dynamicAccess().getClassFor(className, Object.class);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Cannot load serialization-binding class [" + className + "]", e);
        }
    }

    private boolean checkGoogleProtobuf(String className) {
        return checkClass("com.google.protobuf", className);
    }

    private boolean checkScalaPb(String className) {
        return checkClass("scalapb.GeneratedMessage", className);
    }

    private boolean checkClass(String prefix, String className) {
        return !className.startsWith(prefix) || system.dynamicAccess().classIsOnClasspath(className);
    }

    private static List<ClassSerializer> sort(List<ClassSerializer> in) {
        List<ClassSerializer> buf = new ArrayList<>(in.size());
        for (ClassSerializer ca : in) {
            int insertIdx = -1;
            for (int i = 0; i < buf.size(); i++) {
                if (buf.get(i).clazz().isAssignableFrom(ca.clazz())) {
                    insertIdx = i;
                    break;
                }
            }
            if (insertIdx == -1) {
                buf.add(ca);
            } else {
                buf.add(insertIdx, ca);
            }
        }
        return Collections.unmodifiableList(buf);
    }

    private void ensureOnlyAllowedSerializers(Iterable<Serializer> iter) {
        if (!system.settings().AllowJavaSerialization) {
            for (Serializer ser : iter) {
                if (isDisallowedJavaSerializer(ser)) {
                    throw new IllegalArgumentException("Disallowed JavaSerializer binding.");
                }
            }
        }
    }

    private boolean isDisallowedJavaSerializer(Serializer serializer) {
        return serializer instanceof JavaSerializer && !system.settings().AllowJavaSerialization;
    }

    private boolean warnUnexpectedNonTapioSerializer(Class<?> clazz, Serializer ser) {
        String clazzName = clazz.getName();
        if (clazzName.startsWith(TAPIO_PACKAGE_PREFIX) && !ser.getClass().getName().startsWith(TAPIO_PACKAGE_PREFIX)) {
            log.warning(
                    "Using serializer [{}] for message [{}]. Note that this serializer is not implemented by Tapio. "
                            + "It's not recommended to replace serializers for messages provided by Tapio.",
                    ser.getClass().getName(),
                    clazzName);
            return true;
        }
        return false;
    }

    @InternalApi
    boolean shouldWarnAboutJavaSerializer(Class<?> serializedClass, Serializer serializer) {
        boolean suppress = !isWarningOnNoVerificationEnabled
                && NoSerializationVerificationNeeded.class.isAssignableFrom(serializedClass);
        return isJavaSerializationWarningEnabled
                && (serializer instanceof JavaSerializer || serializer instanceof DisabledJavaSerializer)
                && !serializedClass.getName().startsWith(TAPIO_PACKAGE_PREFIX)
                && !serializedClass.getName().startsWith("java.lang.")
                && !suppress;
    }
}
