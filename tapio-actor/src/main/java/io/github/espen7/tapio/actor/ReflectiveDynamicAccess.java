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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于反射与 {@link MethodHandle} 的默认 {@link DynamicAccess} 实现
 * {@code ReflectiveDynamicAccess}。
 *
 * <p>该类不供应用代码继承或直接构造。</p>
 */
public class ReflectiveDynamicAccess extends DynamicAccess {
    private final ClassLoader classLoader;
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
    private final MethodType genericConstructorType = MethodType.methodType(Object.class, Object[].class);
    private final ConcurrentHashMap<Class<?>, ConcurrentHashMap<MethodType, MethodHandle>> constructorHandles =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, VarHandle> moduleHandles = new ConcurrentHashMap<>();

    /**
     * 使用指定类加载器构造默认 {@link DynamicAccess}。
     *
     * @param classLoader 用于解析类的类加载器
     */
    public ReflectiveDynamicAccess(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> getClassFor(String fqcn, Class<T> targetType) throws ClassNotFoundException {
        Class<?> c = Class.forName(fqcn, false, classLoader);
        if (targetType.isAssignableFrom(c)) {
            return (Class<? extends T>) c;
        }
        throw new ClassCastException(targetType + " is not assignable from " + c);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T createInstanceFor(Class<?> clazz, Class<T> targetType,
            List<Map.Entry<Class<?>, Object>> args) throws Throwable {
        Class<?>[] types = new Class<?>[args.size()];
        Object[] values = new Object[args.size()];
        for (int i = 0; i < args.size(); i++) {
            Map.Entry<Class<?>, Object> argument = args.get(i);
            types[i] = argument.getKey();
            values[i] = argument.getValue();
        }
        MethodType methodType = MethodType.methodType(Void.TYPE, types);
        MethodHandle handle = constructorHandle(clazz, methodType);
        Object obj = handle.invokeExact(values);
        if (targetType.isInstance(obj)) {
            return (T) obj;
        }
        throw new ClassCastException(clazz.getName() + " is not a subtype of " + targetType);
    }

    @Override
    public <T> T createInstanceFor(String fqcn, Class<T> targetType,
            List<Map.Entry<Class<?>, Object>> args) throws Throwable {
        Class<? extends T> c = getClassFor(fqcn, targetType);
        return createInstanceFor(c, targetType, args);
    }

    @Override
    public boolean classIsOnClasspath(String fqcn) {
        try {
            getClassFor(fqcn, Object.class);
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObjectFor(String fqcn, Class<T> targetType) throws Throwable {
        Class<?> clazz;
        if (fqcn.endsWith("$")) {
            clazz = getClassFor(fqcn, Object.class);
        } else {
            try {
                clazz = getClassFor(fqcn + "$", Object.class);
            } catch (Throwable ignored) {
                clazz = getClassFor(fqcn, Object.class);
            }
        }
        VarHandle moduleHandle = moduleHandleFor(clazz);
        Object value = moduleHandle.get();
        if (value == null) {
            throw new NullPointerException();
        }
        if (!targetType.isInstance(value)) {
            throw new ClassCastException(fqcn + " is not a subtype of " + targetType);
        }
        return (T) value;
    }

    @Override
    public ClassLoader classLoader() {
        return classLoader;
    }

    private MethodHandle constructorHandle(Class<?> clazz, MethodType methodType) throws Throwable {
        ConcurrentHashMap<MethodType, MethodHandle> existingByType = constructorHandles.get(clazz);
        ConcurrentHashMap<MethodType, MethodHandle> byType;
        if (existingByType != null) {
            byType = existingByType;
        } else {
            ConcurrentHashMap<MethodType, MethodHandle> fresh = new ConcurrentHashMap<>();
            ConcurrentHashMap<MethodType, MethodHandle> existing = constructorHandles.putIfAbsent(clazz, fresh);
            byType = existing == null ? fresh : existing;
        }
        MethodHandle cached = byType.get(methodType);
        if (cached != null) {
            return cached;
        }
        MethodHandle rawHandle = findConstructor(clazz, methodType);
        MethodHandle handle = rawHandle
                .asFixedArity()
                .asSpreader(Object[].class, methodType.parameterCount())
                .asType(genericConstructorType);
        MethodHandle existing = byType.putIfAbsent(methodType, handle);
        return existing == null ? handle : existing;
    }

    private MethodHandle findConstructor(Class<?> clazz, MethodType methodType) throws Throwable {
        try {
            return lookup.findConstructor(clazz, methodType);
        } catch (IllegalAccessException ignored) {
            try {
                return publicLookup.findConstructor(clazz, methodType);
            } catch (IllegalAccessException ignoredAgain) {
                return MethodHandles.privateLookupIn(clazz, lookup).findConstructor(clazz, methodType);
            }
        }
    }

    private VarHandle moduleHandleFor(Class<?> clazz) throws Throwable {
        VarHandle cached = moduleHandles.get(clazz);
        if (cached != null) {
            return cached;
        }
        VarHandle handle = findModuleHandle(clazz);
        VarHandle existing = moduleHandles.putIfAbsent(clazz, handle);
        return existing == null ? handle : existing;
    }

    private VarHandle findModuleHandle(Class<?> clazz) throws Throwable {
        try {
            return lookup.findStaticVarHandle(clazz, "MODULE$", clazz);
        } catch (IllegalAccessException ignored) {
            try {
                return publicLookup.findStaticVarHandle(clazz, "MODULE$", clazz);
            } catch (IllegalAccessException ignoredAgain) {
                return MethodHandles.privateLookupIn(clazz, lookup).findStaticVarHandle(clazz, "MODULE$", clazz);
            }
        }
    }
}
