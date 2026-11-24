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
package io.github.espen7.stardust.japi.pf;

import io.github.espen7.stardust.actor.AbstractActor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Java API 的 Receive 构建器。
 *
 * <p>class cases 按添加顺序检查，首个匹配的 case 执行后即结束匹配。没有匹配项时，
 * 使用 matchAny 注册的兜底处理器；没有兜底处理器时调用 Actor#unhandled。</p>
 */
public final class ReceiveBuilder {

    private final List<Case> cases = new ArrayList<>();
    private Consumer<Object> any;

    private record Case(Class<?> type, Consumer<Object> action) {
    }

    /**
     * 创建空的 ReceiveBuilder。
     */
    public static ReceiveBuilder create() {
        return new ReceiveBuilder();
    }

    /**
     * 注册一个按消息 class 匹配的处理器。
     *
     * @param type 消息类型；匹配使用 {@link Class#isInstance(Object)}，包括子类实例
     * @param action 匹配成功后的消息处理器
     * @param <T> 消息类型
     * @return 当前 builder，便于继续链式注册
     */
    public <T> ReceiveBuilder match(Class<T> type, Consumer<T> action) {
        cases.add(new Case(type, value -> action.accept(type.cast(value))));
        return this;
    }

    /**
     * 注册兜底处理器。
     *
     * @param action 没有 class case 匹配时调用的处理器
     * @return 当前 builder
     */
    public ReceiveBuilder matchAny(Consumer<Object> action) {
        any = action;
        return this;
    }

    /**
     * 构造不可变的 Receive 快照。
     *
     * <p>之后对该 builder 的修改不会改变已构建的 Receive。</p>
     *
     * @return 可由 AbstractActor 返回给 ActorCell 的 Receive
     */
    public AbstractActor.Receive build() {
        return new AbstractActor.Receive((message, cell) -> {
            for (Case entry : cases) {
                if (entry.type().isInstance(message)) {
                    entry.action().accept(message);
                    return;
                }
            }
            if (any != null) any.accept(message);
            else cell.actor().unhandled(message);
        });
    }
}
