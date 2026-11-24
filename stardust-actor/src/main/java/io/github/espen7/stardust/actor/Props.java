package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.japi.function.Creator;

import java.lang.reflect.Constructor;

/**
 * Actor 创建配置，也是创建 Actor 实例的延迟工厂。
 *
 * <p>Props 是 immutable、可共享的描述对象。它保存 Actor class 或 Creator，
 * ActorCell 每次 create/recreate 都通过 newActor() 获取新的实例。</p>
 */
public final class Props {
    private final Class<? extends Actor> clazz;
    private final Creator<? extends Actor> creator;

    private Props(Class<? extends Actor> clazz, Creator<? extends Actor> creator) {
        this.clazz = clazz;
        this.creator = creator;
    }

    /**
     * 使用 Actor class 的无参 constructor 创建 Props。
     *
     * @param clazz 要创建的 Actor class
     * @return 保存该 class 的 Props
     */
    public static Props create(Class<? extends Actor> clazz) {
        return new Props(clazz, () -> {
            Constructor<? extends Actor> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        });
    }

    /**
     * 使用 constructor 参数创建 Props。
     *
     * <p>创建 Actor 时先按参数数量寻找 constructor，再由反射调用检查参数是否匹配。</p>
     *
     * @param clazz 要创建的 Actor class
     * @param args 传给 constructor 的参数
     * @return 保存 class 和参数的 Props
     * @throws IllegalArgumentException 当没有参数数量匹配的 constructor
     * @throws ActorInitializationException 当反射调用 constructor 失败
     */
    public static Props create(Class<? extends Actor> clazz, Object... args) {
        return new Props(clazz, () -> {
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                if (constructor.getParameterCount() == args.length) {
                    constructor.setAccessible(true);
                    return clazz.cast(constructor.newInstance(args));
                }
            }
            throw new IllegalArgumentException("No constructor for " + clazz.getName());
        });
    }

    /**
     * 使用 Creator 创建 Props，适合 lambda 和自定义构造逻辑。
     *
     * @param clazz Actor class
     * @param creator 每次创建 Actor 实例时调用的工厂
     * @param <T> Actor 类型
     * @return 保存 Creator 的 Props
     */
    public static <T extends Actor> Props create(Class<T> clazz, Creator<T> creator) {
        return new Props(clazz, creator);
    }

    /**
     * INTERNAL API：由 ActorCell 调用，每次调用都应产生新的 Actor 实例。
     */
    Actor newActor() {
        try {
            return creator.create();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ActorInitializationException(e);
        }
    }

    /**
     * 返回 Props 声明的 Actor class。
     *
     * @return Actor class
     */
    public Class<? extends Actor> actorClass() {
        return clazz;
    }
}
