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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.Actor;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.Warning;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按配置、Props 和 requirement 选择 mailbox type，对应上游
 * {@code dispatch/Mailboxes.scala} 的 {@code Mailboxes}。
 */
public final class Mailboxes {
    /** 默认 Mailbox 的 ID，也是其完整配置路径。 */
    public static final String DefaultMailboxId = ActorPath.Scheme + ".actor.default-mailbox";
    /** 未设置 mailbox requirement 时的哨兵值。 */
    public static final String NoMailboxRequirement = "";
    /** 由容量直接构造有界 mailbox 的 id 前缀。 */
    public static final String BoundedCapacityPrefix = "bounded-capacity:";

    private final ActorSystem.Settings settings;
    private final EventStream eventStream;
    private final Map<Class<?>, String> mailboxBindings;
    private final ConcurrentHashMap<String, MailboxType> mailboxTypeConfigurators = new ConcurrentHashMap<>();
    private final Config defaultMailboxConfig;
    private volatile boolean mailboxNonZeroPushTimeoutWarningIssued;

    /**
     * 创建 mailbox 注册表。
     *
     * @param settings 所属系统的有效配置
     * @param eventStream 接收 mailbox 警告事件的 EventStream
     */
    public Mailboxes(ActorSystem.Settings settings, EventStream eventStream) {
        this.settings = settings;
        this.eventStream = eventStream;
        this.defaultMailboxConfig = settings.config().getConfig(DefaultMailboxId);
        this.mailboxBindings = loadMailboxBindings();
    }

    /**
     * 按 id 查找 mailbox type。
     *
     * @param id mailbox 配置路径
     * @return 对应的 mailbox type
     */
    public MailboxType lookup(String id) {
        return lookupConfigurator(id);
    }

    /**
     * 按消息队列语义类型查找 mailbox type。
     *
     * @param queueType 消息队列语义类型
     * @return 对应的 mailbox type
     */
    public MailboxType lookupByQueueType(Class<?> queueType) {
        return lookup(lookupId(queueType));
    }

    /**
     * 返回 Actor class 要求的消息队列语义类型。
     *
     * @param actorClass Actor class
     * @return 要求的语义类型
     * @throws IllegalArgumentException 当标记参数不是具体 class 时
     */
    public Class<?> getRequiredType(Class<? extends Actor> actorClass) {
        Type marker = findMarker(actorClass, RequiresMessageQueue.class);
        if (marker instanceof ParameterizedType parameterized) {
            Type argument = parameterized.getActualTypeArguments()[0];
            if (argument instanceof Class<?> type) return type;
            throw new IllegalArgumentException(
                    "no wildcard type allowed in RequireMessageQueue argument (was [" + argument + "])");
        }
        throw new IllegalArgumentException("Unexpected actor class marker: " + marker);
    }

    /**
     * @param actorClass Actor class
     * @return Actor class 声明了 {@link RequiresMessageQueue} 时返回 {@code true}
     */
    public boolean hasRequiredType(Class<? extends Actor> actorClass) {
        return RequiresMessageQueue.class.isAssignableFrom(actorClass);
    }

    /**
     * 返回 mailbox type 承诺创建的消息队列语义类型。
     *
     * @param mailboxType mailbox type
     * @return 承诺的语义类型；未声明时为 {@link MessageQueue}
     */
    public Class<?> getProducedMessageQueueType(MailboxType mailboxType) {
        if (!(mailboxType instanceof ProducesMessageQueue<?>)) return MessageQueue.class;
        Type marker = findMarker(mailboxType.getClass(), ProducesMessageQueue.class);
        if (marker instanceof ParameterizedType parameterized) {
            Type argument = parameterized.getActualTypeArguments()[0];
            if (argument instanceof Class<?> type) return type;
            throw new IllegalArgumentException(
                    "no wildcard type allowed in ProducesMessageQueue argument (was [" + argument + "])");
        }
        throw new IllegalArgumentException("Unexpected message queue type marker: " + marker);
    }

    /**
     * 按配置、Props 和 Actor requirement 选择 mailbox type，对应上游五级选择顺序。
     *
     * @param props 创建 Actor 用的 Props
     * @param dispatcherConfig 选中的 Dispatcher 的有效配置
     * @return 选中的 mailbox type
     */
    public MailboxType getMailboxType(Props props, Config dispatcherConfig) {
        String id = dispatcherConfig.getString("id");
        Deploy deploy = props.deploy();
        Class<? extends Actor> actorClass = props.actorClass();
        Class<?> actorRequirement = hasRequiredType(actorClass) ? getRequiredType(actorClass) : null;
        Class<?> mailboxRequirement = getMailboxRequirement(dispatcherConfig);
        boolean hasMailboxRequirement = mailboxRequirement != MessageQueue.class;
        boolean hasMailboxType = dispatcherConfig.hasPath("mailbox-type")
                && !Deploy.NoMailboxGiven.equals(dispatcherConfig.getString("mailbox-type"));

        if (!Deploy.NoMailboxGiven.equals(deploy.mailbox())) {
            return verifyRequirements(lookup(deploy.mailbox()), id, actorClass, actorRequirement,
                    mailboxRequirement, hasMailboxRequirement);
        }
        if (!Deploy.NoDispatcherGiven.equals(deploy.dispatcher())
                && !Deploy.DispatcherSameAsParent.equals(deploy.dispatcher())
                && hasMailboxType) {
            return verifyRequirements(lookup(dispatcherConfig.getString("id")), id, actorClass, actorRequirement,
                    mailboxRequirement, hasMailboxRequirement);
        }
        if (actorRequirement != null) {
            try {
                return verifyRequirements(lookupByQueueType(actorRequirement), id, actorClass, actorRequirement,
                        mailboxRequirement, hasMailboxRequirement);
            } catch (RuntimeException missingMapping) {
                if (!hasMailboxRequirement) throw missingMapping;
                return verifyRequirements(lookupByQueueType(mailboxRequirement), id, actorClass, actorRequirement,
                        mailboxRequirement, hasMailboxRequirement);
            }
        }
        if (hasMailboxRequirement) {
            return verifyRequirements(lookupByQueueType(mailboxRequirement), id, actorClass, actorRequirement,
                    mailboxRequirement, hasMailboxRequirement);
        }
        return verifyRequirements(lookup(DefaultMailboxId), id, actorClass, actorRequirement,
                mailboxRequirement, hasMailboxRequirement);
    }

    private MailboxType verifyRequirements(MailboxType mailboxType, String dispatcherId,
                                          Class<? extends Actor> actorClass, Class<?> actorRequirement,
                                          Class<?> mailboxRequirement, boolean hasMailboxRequirement) {
        Class<?> producedType = getProducedMessageQueueType(mailboxType);
        if (hasMailboxRequirement && !mailboxRequirement.isAssignableFrom(producedType)) {
            throw new IllegalArgumentException(
                    "produced message queue type [" + producedType + "] does not fulfill requirement for dispatcher ["
                            + dispatcherId + "]. Must be a subclass of [" + mailboxRequirement + "].");
        }
        if (actorRequirement != null && !actorRequirement.isAssignableFrom(producedType)) {
            throw new IllegalArgumentException(
                    "produced message queue type [" + producedType + "] does not fulfill requirement for actor class ["
                            + actorClass + "]. Must be a subclass of [" + actorRequirement + "].");
        }
        return mailboxType;
    }

    private MailboxType lookupConfigurator(String id) {
        MailboxType existing = mailboxTypeConfigurators.get(id);
        if (existing != null) return existing;
        MailboxType created;
        if ("unbounded".equals(id)) {
            created = new UnboundedMailbox();
        } else if ("bounded".equals(id)) {
            created = new BoundedMailbox(settings, config(id));
        } else if (id.startsWith(BoundedCapacityPrefix)) {
            int capacity = Integer.parseInt(id.split(":")[1]);
            created = new BoundedMailbox(capacity, Duration.ZERO);
        } else {
            if (!settings.config().hasPath(id)) throw new ConfigurationException("Mailbox Type [" + id + "] not configured");
            Config conf = config(id);
            String mailboxType = conf.getString("mailbox-type");
            if (mailboxType.isEmpty()) {
                throw new ConfigurationException("The setting mailbox-type, defined in [" + id + "] is empty");
            }
            created = instantiateMailboxType(mailboxType, id, conf);
            warnOnNonZeroPushTimeout(id, created);
        }
        MailboxType previous = mailboxTypeConfigurators.putIfAbsent(id, created);
        return previous == null ? created : previous;
    }

    private MailboxType instantiateMailboxType(String fqcn, String id, Config conf) {
        try {
            Class<?> type = Class.forName(fqcn, true, settings.config().getClass().getClassLoader());
            return (MailboxType) type.getConstructor(ActorSystem.Settings.class, Config.class)
                    .newInstance(settings, conf);
        } catch (ReflectiveOperationException | ClassCastException exception) {
            throw new IllegalArgumentException(
                    "Cannot instantiate MailboxType [" + fqcn + "], defined in [" + id + "], make sure it has a public"
                            + " constructor with [" + ActorSystem.Settings.class.getName()
                            + ", " + Config.class.getName() + "] parameters", exception);
        }
    }

    private void warnOnNonZeroPushTimeout(String id, MailboxType mailboxType) {
        if (mailboxNonZeroPushTimeoutWarningIssued) return;
        if (mailboxType instanceof ProducesPushTimeoutSemanticsMailbox producesTimeout) {
            Duration pushTimeOut = producesTimeout.pushTimeOut();
            if (pushTimeOut.toNanos() > 0L) {
                eventStream.publish(new Warning("mailboxes", getClass(),
                        "Configured potentially-blocking mailbox [" + id + "] configured with non-zero pushTimeOut ("
                                + pushTimeOut + "), which can lead to blocking behavior when sending messages to this"
                                + " mailbox. Avoid this by setting `" + id + ".mailbox-push-timeout-time` to `0`."));
                mailboxNonZeroPushTimeoutWarningIssued = true;
            }
        }
    }

    private String lookupId(Class<?> queueType) {
        String id = mailboxBindings.get(queueType);
        if (id == null) throw new ConfigurationException("Mailbox Mapping for [" + queueType + "] not configured");
        return id;
    }

    private Class<?> getMailboxRequirement(Config config) {
        String requirement = config.getString("mailbox-requirement");
        if (NoMailboxRequirement.equals(requirement)) return MessageQueue.class;
        try {
            return Class.forName(requirement, true, settings.config().getClass().getClassLoader());
        } catch (ClassNotFoundException exception) {
            throw new ConfigurationException(
                    "Type [" + requirement + "] specified as mailbox-requirement can't be loaded", exception);
        }
    }

    private Config config(String id) {
        return ConfigFactory.parseMap(Map.of("id", id))
                .withFallback(settings.config().getConfig(id))
                .withFallback(defaultMailboxConfig);
    }

    private Map<Class<?>, String> loadMailboxBindings() {
        ClassLoader classLoader = settings.config().getClass().getClassLoader();
        Config requirements = settings.config().getConfig(
                ActorPath.Scheme + ".actor.mailbox.requirements");
        Map<Class<?>, String> bindings = new HashMap<>();
        for (var entry : requirements.root().entrySet()) {
            String className = entry.getKey();
            Object value = entry.getValue().unwrapped();
            try {
                if (!(value instanceof String mailboxId)) {
                    throw new ConfigurationException(
                            "Mailbox mapping for [" + className + "] must be a string but was [" + value + "]");
                }
                bindings.put(Class.forName(className, true, classLoader), mailboxId);
            } catch (ClassNotFoundException exception) {
                throw new ConfigurationException(
                        "Type [" + className + "] specified as " + ActorPath.Scheme
                                + ".actor.mailbox.requirement [" + value + "] in config can't be loaded due to ["
                                + exception.getMessage() + "]", exception);
            }
        }
        return Map.copyOf(bindings);
    }

    private static Type findMarker(Class<?> root, Class<?> marker) {
        Class<?> current = root;
        while (true) {
            Class<?> superclass = current.getSuperclass();
            if (superclass != null && marker.isAssignableFrom(superclass)) {
                current = superclass;
                continue;
            }
            Type found = null;
            for (Type candidate : current.getGenericInterfaces()) {
                Class<?> raw = rawType(candidate);
                if (raw != null && marker.isAssignableFrom(raw)) {
                    found = candidate;
                    break;
                }
            }
            if (found == null) {
                throw new IllegalArgumentException(
                        "cannot find [" + marker + "] in ancestors of [" + root + "]");
            }
            if (rawType(found) == marker) return found;
            current = rawType(found);
        }
    }

    private static Class<?> rawType(Type type) {
        if (type instanceof Class<?> clazz) return clazz;
        if (type instanceof ParameterizedType parameterized) return (Class<?>) parameterized.getRawType();
        return null;
    }
}
