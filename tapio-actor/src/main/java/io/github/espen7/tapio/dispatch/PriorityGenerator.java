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
package io.github.espen7.tapio.dispatch;

import java.util.Comparator;
import java.util.function.ToIntFunction;

/**
 * 为优先级邮箱生成消息优先级数值（数值越小优先级越高），对应 Pekko {@code PriorityGenerator}。
 */
public abstract class PriorityGenerator implements Comparator<Envelope> {
    /** 创建 PriorityGenerator 实例。 */
    protected PriorityGenerator() {
    }

    /**
     * 使用函数式映射创建 {@link PriorityGenerator}。
     *
     * @param priorityFunction 将消息映射为整数优先级的函数
     * @return PriorityGenerator 实例
     */
    public static PriorityGenerator create(ToIntFunction<Object> priorityFunction) {
        return new PriorityGenerator() {
            @Override
            public int gen(Object message) {
                return priorityFunction.applyAsInt(message);
            }
        };
    }

    /**
     * 返回指定消息的优先级；数值越小越优先出队。
     *
     * @param message 消息对象
     * @return 整数优先级
     */
    public abstract int gen(Object message);

    @Override
    public final int compare(Envelope thisMessage, Envelope thatMessage) {
        return Integer.compare(gen(thisMessage.message()), gen(thatMessage.message()));
    }
}
