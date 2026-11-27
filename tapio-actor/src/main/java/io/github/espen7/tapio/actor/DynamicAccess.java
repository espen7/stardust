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

import io.github.espen7.tapio.annotation.DoNotInherit;

import java.util.List;
import java.util.Map;

/**
 * 用于加载与实例化 {@link ActorSystem} 可配置组件的内部抽象，对应 Pekko {@code DynamicAccess}。
 *
 * <p>默认实现为 {@link ReflectiveDynamicAccess}。该类不供应用代码继承。</p>
 */
@DoNotInherit
public abstract class DynamicAccess {
    /** 创建 DynamicAccess 实例。 */
    protected DynamicAccess() {
    }

    /**
     * 使用给定的类与构造参数列表创建实例。
     *
     * @param clazz 目标类
     * @param targetType 期望的父类型或接口
     * @param args 构造参数类型与参数值列表
     * @param <T> 目标类型
     * @return 新建实例
     * @throws Throwable 加载、查找或构造失败时抛出原异常
     */
    public abstract <T> T createInstanceFor(Class<?> clazz, Class<T> targetType,
            List<Map.Entry<Class<?>, Object>> args) throws Throwable;

    /**
     * 按全限定类名加载类并校验其可赋值给 {@code targetType}。
     *
     * @param fqcn 全限定类名
     * @param targetType 期望的父类型或接口
     * @param <T> 目标类型
     * @return 加载得到的 Class 对象
     * @throws ClassNotFoundException 类未找到时
     * @throws ClassCastException 类不可赋值给 {@code targetType} 时
     */
    public abstract <T> Class<? extends T> getClassFor(String fqcn, Class<T> targetType)
            throws ClassNotFoundException;

    /**
     * 检查指定全限定类名是否位于当前类路径中。
     *
     * @param fqcn 全限定类名
     * @return 类存在且可加载时返回 {@code true}
     */
    public abstract boolean classIsOnClasspath(String fqcn);

    /**
     * 按全限定类名加载类并通过指定参数创建实例。
     *
     * @param fqcn 全限定类名
     * @param targetType 期望的父类型或接口
     * @param args 构造参数类型与参数值列表
     * @param <T> 目标类型
     * @return 新建实例
     * @throws Throwable 加载、查找或构造失败时抛出原异常
     */
    public abstract <T> T createInstanceFor(String fqcn, Class<T> targetType,
            List<Map.Entry<Class<?>, Object>> args) throws Throwable;

    /**
     * 获取指定全限定类名对应的单例对象实例（如静态 {@code MODULE$} 字段）。
     *
     * @param fqcn 全限定类名
     * @param targetType 期望的父类型或接口
     * @param <T> 目标类型
     * @return 单例对象实例
     * @throws Throwable 加载、查找或类型校验失败时抛出原异常
     */
    public abstract <T> T getObjectFor(String fqcn, Class<T> targetType) throws Throwable;

    /**
     * 返回此 DynamicAccess 使用的 {@link ClassLoader}。
     *
     * @return 类加载器
     */
    public abstract ClassLoader classLoader();
}
