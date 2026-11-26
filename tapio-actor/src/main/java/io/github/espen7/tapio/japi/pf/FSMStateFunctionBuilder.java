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

import io.github.espen7.tapio.actor.FSM;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * 用于构建 {@link io.github.espen7.tapio.actor.AbstractFSM} 状态处理函数的流式构建器，
 * 对应 Pekko {@code FSMStateFunctionBuilder}。
 *
 * @param <S> 状态类型
 * @param <D> 状态数据类型
 */
public final class FSMStateFunctionBuilder<S, D> {
    private record Rule<S, D>(Predicate<FSM.Event<D>> matcher, BiFunction<Object, D, FSM.State<S, D>> handler) {
    }

    private final List<Rule<S, D>> rules = new ArrayList<>();

    /** 创建状态函数构建器。 */
    public FSMStateFunctionBuilder() {
    }

    /**
     * 匹配指定事件类型的消息。
     */
    @SuppressWarnings("unchecked")
    public <P> FSMStateFunctionBuilder<S, D> matchEvent(
            Class<P> eventType, BiFunction<P, D, FSM.State<S, D>> apply) {
        Objects.requireNonNull(eventType);
        Objects.requireNonNull(apply);
        rules.add(new Rule<>(
                ev -> eventType.isInstance(ev.event()),
                (event, data) -> apply.apply((P) event, data)));
        return this;
    }

    /**
     * 匹配指定事件类型且满足特定条件的消息。
     */
    @SuppressWarnings("unchecked")
    public <P> FSMStateFunctionBuilder<S, D> matchEvent(
            Class<P> eventType, Predicate<P> predicate, BiFunction<P, D, FSM.State<S, D>> apply) {
        Objects.requireNonNull(eventType);
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(apply);
        rules.add(new Rule<>(
                ev -> eventType.isInstance(ev.event()) && predicate.test((P) ev.event()),
                (event, data) -> apply.apply((P) event, data)));
        return this;
    }

    /**
     * 匹配与给定对象相等的事件（例如 {@link FSM#StateTimeout}）。
     */
    public FSMStateFunctionBuilder<S, D> matchEventEquals(
            Object event, BiFunction<Object, D, FSM.State<S, D>> apply) {
        Objects.requireNonNull(apply);
        rules.add(new Rule<>(
                ev -> Objects.equals(ev.event(), event),
                apply));
        return this;
    }

    /**
     * 匹配任意未命中前面规则的事件。
     */
    public FSMStateFunctionBuilder<S, D> matchAnyEvent(BiFunction<Object, D, FSM.State<S, D>> apply) {
        Objects.requireNonNull(apply);
        rules.add(new Rule<>(ev -> true, apply));
        return this;
    }

    /** 构建不可变 {@link PartialFunction}。 */
    public PartialFunction<FSM.Event<D>, FSM.State<S, D>> build() {
        List<Rule<S, D>> snapshot = List.copyOf(rules);
        return new PartialFunction<>() {
            @Override
            public boolean isDefinedAt(FSM.Event<D> event) {
                for (Rule<S, D> rule : snapshot) {
                    if (rule.matcher().test(event)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public FSM.State<S, D> apply(FSM.Event<D> event) {
                for (Rule<S, D> rule : snapshot) {
                    if (rule.matcher().test(event)) {
                        return rule.handler().apply(event.event(), event.stateData());
                    }
                }
                throw new IllegalArgumentException("Unhandled FSM event: " + event);
            }
        };
    }
}
