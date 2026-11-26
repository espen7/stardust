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

import java.util.function.Function;

/**
 * Scala PartialFunction 在 Java 中的对应类型。只有 {@link #isDefinedAt(Object)}
 * 返回 true 的输入才属于该函数的定义域。
 *
 * @param <F> 输入类型
 * @param <T> 结果类型
 */
public interface PartialFunction<F, T> {
    boolean isDefinedAt(F value);

    T apply(F value) throws Exception;

    /**
     * 执行首个匹配或使用 fallback。结果类型应能同时表示当前偏函数与 fallback 的结果。
     *
     * @param value 待匹配的输入
     * @param fallback 输入不在定义域时执行的函数
     * @param <R> 两个分支的共同结果类型
     * @return 所选分支的结果
     * @throws Exception 匹配或执行期间的异常
     */
    @SuppressWarnings("unchecked")
    default <R> R applyOrElse(F value, Function<? super F, ? extends R> fallback) throws Exception {
        return isDefinedAt(value) ? (R) apply(value) : fallback.apply(value);
    }

    default PartialFunction<F, T> orElse(PartialFunction<F, T> other) {
        PartialFunction<F, T> first = this;
        return new PartialFunction<>() {
            private final Object fallbackMarker = new Object();

            @Override
            public boolean isDefinedAt(F value) {
                return first.isDefinedAt(value) || other.isDefinedAt(value);
            }

            @Override
            @SuppressWarnings("unchecked")
            public T apply(F value) throws Exception {
                Object result = first.applyOrElse(value, ignored -> fallbackMarker);
                return result == fallbackMarker ? other.apply(value) : (T) result;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <R> R applyOrElse(F value, Function<? super F, ? extends R> fallback) throws Exception {
                Object result = first.applyOrElse(value, ignored -> fallbackMarker);
                return result == fallbackMarker ? other.applyOrElse(value, fallback) : (R) result;
            }
        };
    }

    static <F, T> PartialFunction<F, T> empty() {
        return new PartialFunction<>() {
            @Override
            public boolean isDefinedAt(F value) {
                return false;
            }

            @Override
            public T apply(F value) {
                throw new UnsupportedOperationException("Empty behavior apply()");
            }
        };
    }
}
