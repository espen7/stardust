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
package io.github.espen7.tapio.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记仅供 Tapio runtime 内部使用、可能随时变化且不承诺兼容性的 API。
 *
 * <p>当 Java 类型需要表达 Pekko Scala {@code private[pekko]} 的内部边界时，
 * 使用此注解保留该 API 意图。Java 没有对应的包层级可见性，因此该注解本身
 * 不改变 Java 的访问控制。</p>
 */
@Documented
@Retention(RetentionPolicy.CLASS) // to be accessible by MiMa
@Target({
        ElementType.METHOD,
        ElementType.CONSTRUCTOR,
        ElementType.FIELD,
        ElementType.TYPE,
        ElementType.PACKAGE
})
public @interface InternalApi {
}
