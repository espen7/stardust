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
package io.github.espen7.tapio.japi.function;

import java.io.Serializable;

/** 判断输入是否符合匹配条件。 */
@FunctionalInterface
public interface Predicate<T> extends Serializable {
    boolean test(T param);

    default Predicate<T> negate() {
        return param -> !test(param);
    }
}
