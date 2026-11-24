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
        Object[] constructorArgs = args.clone();
        return new Props(clazz, () -> {
            Constructor<?> selected = null;
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                if (!matches(constructor.getParameterTypes(), constructorArgs)) continue;
                if (selected != null) {
                    throw new IllegalArgumentException("multiple matching constructors found on " + clazz
                            + " for arguments [" + argumentTypes(constructorArgs) + "]");
                }
                selected = constructor;
            }
            if (selected == null) {
                throw new IllegalArgumentException("no matching constructor found on " + clazz
                        + " for arguments [" + argumentTypes(constructorArgs) + "]");
            }
            selected.setAccessible(true);
            return clazz.cast(selected.newInstance(constructorArgs));
        });
    }

    private static boolean matches(Class<?>[] parameterTypes, Object[] arguments) {
        if (parameterTypes.length != arguments.length) return false;
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            Object argument = arguments[i];
            if (argument == null) {
                if (parameterType.isPrimitive()) return false;
            } else if (!box(parameterType).isInstance(argument)) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> box(Class<?> type) {
        if (!type.isPrimitive()) return type;
        if (type == boolean.class) return Boolean.class;
        if (type == byte.class) return Byte.class;
        if (type == short.class) return Short.class;
        if (type == char.class) return Character.class;
        if (type == int.class) return Integer.class;
        if (type == long.class) return Long.class;
        if (type == float.class) return Float.class;
        if (type == double.class) return Double.class;
        return Void.class;
    }

    private static String argumentTypes(Object[] arguments) {
        return java.util.Arrays.stream(arguments)
                .map(argument -> argument == null ? "null" : argument.getClass().getName())
                .collect(java.util.stream.Collectors.joining(", "));
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
