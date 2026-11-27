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
package io.github.espen7.tapio.japi.pf;

import io.github.espen7.tapio.actor.AbstractActor.Receive;
import io.github.espen7.tapio.japi.function.Predicate;
import io.github.espen7.tapio.japi.function.Procedure;

import java.util.function.BooleanSupplier;

/** 按添加顺序构造 Actor 的消息匹配逻辑。 */
public class ReceiveBuilder {
    private PartialFunction<Object, Void> statements;

    protected void addStatement(PartialFunction<Object, Void> statement) {
        statements = statements == null ? statement : statements.orElse(statement);
    }

    /**
     * 捕获当前的匹配分支。继续修改 builder 不会改变已构建的行为。
     *
     * @return 匹配首个符合条件的分支的 Receive
     */
    public Receive build() {
        PartialFunction<Object, Void> empty = PartialFunction.empty();
        return new Receive(statements == null ? empty : statements.orElse(empty));
    }

    /** @return 新的消息匹配构建器 */
    public static ReceiveBuilder create() {
        return new ReceiveBuilder();
    }

    /**
     * 追加匹配指定类型及其子类型的分支。
     *
     * @param type 消息类型
     * @param apply 匹配成功后执行的操作
     * @param <P> 消息类型
     * @return 当前 builder
     */
    public <P> ReceiveBuilder match(Class<P> type, Procedure<P> apply) {
        return matchUnchecked(type, apply);
    }

    /**
     * 追加匹配指定类型的分支，不做编译期泛型类型校验。
     *
     * @param type 消息类型
     * @param apply 匹配成功后执行的操作
     * @return 当前 builder
     */
    @SuppressWarnings("unchecked")
    public ReceiveBuilder matchUnchecked(Class<?> type, Procedure<?> apply) {
        addStatement(new UnitCaseStatement<>(type::isInstance, (Procedure<Object>) apply));
        return this;
    }

    /**
     * 追加同时满足类型和条件的分支。
     *
     * @param type 消息类型
     * @param predicate 类型匹配后检查的条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 消息类型
     * @return 当前 builder
     */
    public <P> ReceiveBuilder match(Class<P> type, Predicate<P> predicate, Procedure<P> apply) {
        return matchUnchecked(type, predicate, apply);
    }

    /**
     * 追加同时满足类型和外部条件的分支。
     *
     * @param type 消息类型
     * @param externalPredicate 外部条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 消息类型
     * @return 当前 builder
     */
    public <P> ReceiveBuilder match(Class<P> type, BooleanSupplier externalPredicate, Procedure<P> apply) {
        return matchUnchecked(type, externalPredicate, apply);
    }

    /**
     * 追加同时满足类型和条件的分支，不做编译期泛型类型校验。
     *
     * @param type 消息类型
     * @param predicate 类型匹配后检查的条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 消息类型
     * @return 当前 builder
     */
    @SuppressWarnings("unchecked")
    public <P> ReceiveBuilder matchUnchecked(Class<?> type, Predicate<?> predicate, Procedure<P> apply) {
        Predicate<Object> fiPredicate = value -> type.isInstance(value) && ((Predicate<Object>) predicate).test(value);
        addStatement(new UnitCaseStatement<>(fiPredicate, (Procedure<Object>) apply));
        return this;
    }

    /**
     * 追加同时满足类型和外部条件的分支，不做编译期泛型类型校验。
     *
     * @param type 消息类型
     * @param externalPredicate 外部条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 消息类型
     * @return 当前 builder
     */
    @SuppressWarnings("unchecked")
    public <P> ReceiveBuilder matchUnchecked(Class<?> type, BooleanSupplier externalPredicate, Procedure<P> apply) {
        Predicate<Object> fiPredicate = value -> type.isInstance(value) && externalPredicate.getAsBoolean();
        addStatement(new UnitCaseStatement<>(fiPredicate, (Procedure<Object>) apply));
        return this;
    }

    /**
     * 追加按 {@code equals} 精确匹配对象的分支。
     *
     * @param object 期望匹配的对象
     * @param apply 匹配成功后执行的操作
     * @param <P> 对象类型
     * @return 当前 builder
     */
    public <P> ReceiveBuilder matchEquals(P object, Procedure<P> apply) {
        addStatement(new UnitCaseStatement<Object, P>(object::equals, apply));
        return this;
    }

    /**
     * 追加按 {@code equals} 与附加条件匹配对象的分支。
     *
     * @param object 期望匹配的对象
     * @param predicate 附加条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 对象类型
     * @return 当前 builder
     */
    @SuppressWarnings("unchecked")
    public <P> ReceiveBuilder matchEquals(P object, Predicate<P> predicate, Procedure<P> apply) {
        addStatement(new UnitCaseStatement<Object, P>(
                value -> object.equals(value) && predicate.test((P) value), apply));
        return this;
    }

    /**
     * 追加按 {@code equals} 与外部条件匹配对象的分支。
     *
     * @param object 期望匹配的对象
     * @param externalPredicate 外部条件
     * @param apply 匹配成功后执行的操作
     * @param <P> 对象类型
     * @return 当前 builder
     */
    @SuppressWarnings("unchecked")
    public <P> ReceiveBuilder matchEquals(P object, BooleanSupplier externalPredicate, Procedure<P> apply) {
        addStatement(new UnitCaseStatement<>(value -> object.equals(value) && externalPredicate.getAsBoolean(),
                (Procedure<Object>) apply));
        return this;
    }

    /**
     * 追加匹配任意消息的分支。该分支位于当前匹配序列的末尾。
     *
     * @param apply 匹配成功后执行的操作
     * @return 当前 builder
     */
    public ReceiveBuilder matchAny(Procedure<Object> apply) {
        addStatement(new UnitCaseStatement<>(value -> true, apply));
        return this;
    }

    /**
     * 追加满足外部条件时匹配任意消息的分支。
     *
     * @param externalPredicate 外部条件
     * @param apply 匹配成功后执行的操作
     * @return 当前 builder
     */
    public ReceiveBuilder matchAny(BooleanSupplier externalPredicate, Procedure<Object> apply) {
        addStatement(new UnitCaseStatement<>(value -> externalPredicate.getAsBoolean(), apply));
        return this;
    }
}
