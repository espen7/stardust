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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * 用于构建 {@link io.github.espen7.tapio.actor.AbstractFSM} 状态转换回调的流式构建器。
 *
 * @param <S> 状态类型
 */
public final class FSMTransitionHandlerBuilder<S> {
    private record TransitionRule<S>(BiPredicate<S, S> matcher, BiConsumer<S, S> action) {
    }

    private final List<TransitionRule<S>> rules = new ArrayList<>();

    /** 创建状态转换回调构建器。 */
    public FSMTransitionHandlerBuilder() {
    }

    /**
     * 匹配从 {@code fromState} 到 {@code toState} 的转换（传入 {@code null} 表示通配任意状态）。
     */
    public FSMTransitionHandlerBuilder<S> state(S fromState, S toState, Runnable action) {
        Objects.requireNonNull(action);
        return state(fromState, toState, (f, t) -> action.run());
    }

    /**
     * 匹配从 {@code fromState} 到 {@code toState} 的转换（传入 {@code null} 表示通配任意状态）。
     */
    public FSMTransitionHandlerBuilder<S> state(S fromState, S toState, BiConsumer<S, S> action) {
        Objects.requireNonNull(action);
        rules.add(new TransitionRule<>(
                (f, t) -> (fromState == null || Objects.equals(fromState, f))
                        && (toState == null || Objects.equals(toState, t)),
                action));
        return this;
    }

    /** 构建组合后的转换处理函数。 */
    public BiConsumer<S, S> build() {
        List<TransitionRule<S>> snapshot = List.copyOf(rules);
        return (fromState, toState) -> {
            for (TransitionRule<S> rule : snapshot) {
                if (rule.matcher().test(fromState, toState)) {
                    rule.action().accept(fromState, toState);
                    return;
                }
            }
        };
    }
}
