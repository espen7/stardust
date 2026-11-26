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

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.internal.ScalaBatchable;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * INTERNAL API：把当前任务中嵌套提交的 Batchable 合并到同一个执行批次。
 */
@InternalApi
abstract class BatchingExecutor implements Executor {
    private final ThreadLocal<AbstractBatch> _tasksLocal = new ThreadLocal<>();
    private final ThreadLocal<BlockContext> _blockContext = new ThreadLocal<>();

    private abstract class AbstractBatch extends ArrayDeque<Runnable> implements Runnable {
        AbstractBatch() {
            super(4);
        }

        final void processBatch(AbstractBatch batch) {
            while (batch == this && !isEmpty()) {
                pollFirst().run();
                batch = _tasksLocal.get();
            }
        }

        protected final boolean resubmitUnbatched() {
            AbstractBatch current = _tasksLocal.get();
            _tasksLocal.remove();
            if (current == this && !isEmpty()) {
                unbatchedExecute(this);
                return true;
            }
            return false;
        }
    }

    private final class Batch extends AbstractBatch {
        @Override
        public final void run() {
            AbstractBatch previousBatch = _tasksLocal.get();
            _tasksLocal.set(this);
            try {
                processBatch(this);
            } catch (Throwable t) {
                resubmitUnbatched();
                throw t;
            } finally {
                if (previousBatch == null) _tasksLocal.remove();
                else _tasksLocal.set(previousBatch);
            }
        }
    }

    private final class BlockableBatch extends AbstractBatch implements BlockContext {
        @Override
        public final void run() {
            AbstractBatch previousBatch = _tasksLocal.get();
            _tasksLocal.set(this);
            boolean firstInvocation = _blockContext.get() == null;
            if (firstInvocation) _blockContext.set(BlockContext.current());
            BlockContext.withBlockContext(this, () -> {
                try {
                    processBatch(this);
                } catch (Throwable t) {
                    resubmitUnbatched();
                    throw t;
                } finally {
                    if (previousBatch == null) _tasksLocal.remove();
                    else _tasksLocal.set(previousBatch);
                    if (firstInvocation) _blockContext.remove();
                }
                return null;
            });
        }

        @Override
        public <T> T blockOn(Supplier<T> thunk, CanAwait permission) {
            resubmitUnbatched();
            return _blockContext.get().blockOn(thunk, permission);
        }
    }

    protected abstract void unbatchedExecute(Runnable r);

    protected abstract boolean resubmitOnBlock();

    @Override
    public void execute(Runnable runnable) {
        if (batchable(runnable)) {
            AbstractBatch batch = _tasksLocal.get();
            if (batch == null) {
                AbstractBatch newBatch = resubmitOnBlock() ? new BlockableBatch() : new Batch();
                newBatch.add(runnable);
                unbatchedExecute(newBatch);
            } else {
                batch.add(runnable);
            }
        } else {
            unbatchedExecute(runnable);
        }
    }

    public boolean batchable(Runnable runnable) {
        return ScalaBatchable.isBatchable(runnable);
    }
}
