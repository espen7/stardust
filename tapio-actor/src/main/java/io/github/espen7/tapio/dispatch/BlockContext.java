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

import java.util.function.Supplier;

/** 对应 Scala {@code BlockContext} 的线程阻塞上下文。 */
interface BlockContext {
    BlockContext DEFAULT = new BlockContext() {
        @Override
        public <T> T blockOn(Supplier<T> thunk, CanAwait permission) {
            return thunk.get();
        }
    };

    static BlockContext current() {
        BlockContext current = Holder.CURRENT.get();
        return current == null ? DEFAULT : current;
    }

    static <T> T withBlockContext(BlockContext context, Supplier<T> thunk) {
        BlockContext previous = Holder.CURRENT.get();
        Holder.CURRENT.set(context);
        try {
            return thunk.get();
        } finally {
            if (previous == null) Holder.CURRENT.remove();
            else Holder.CURRENT.set(previous);
        }
    }

    <T> T blockOn(Supplier<T> thunk, CanAwait permission);

    final class Holder {
        private static final ThreadLocal<BlockContext> CURRENT = new ThreadLocal<>();

        private Holder() {
        }
    }
}
