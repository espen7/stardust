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

import io.github.espen7.tapio.japi.function.Predicate;
import io.github.espen7.tapio.japi.function.Procedure;

/** 把条件和无返回值的处理器组合为一个消息匹配分支。 */
final class UnitCaseStatement<F, P> implements PartialFunction<F, Void> {
    private final Predicate<F> predicate;
    private final Procedure<P> apply;

    UnitCaseStatement(Predicate<F> predicate, Procedure<P> apply) {
        this.predicate = predicate;
        this.apply = apply;
    }

    @Override
    public boolean isDefinedAt(F value) {
        return predicate.test(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void apply(F value) throws Exception {
        apply.apply((P) value);
        return null;
    }
}
