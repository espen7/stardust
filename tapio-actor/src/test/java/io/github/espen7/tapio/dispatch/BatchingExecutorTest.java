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

import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.LogEventException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchingExecutorTest {
    private ActorSystem system;

    @AfterEach
    void closeSystem() {
        if (system != null) system.close();
    }

    @Test
    void nestedBatchableTasksShareOneUnderlyingInvocation() {
        system = ActorSystem.create("batching-nested");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        List<String> calls = new ArrayList<>();

        dispatcher.execute(batchable(() -> {
            calls.add("outer");
            dispatcher.execute(batchable(() -> calls.add("first")));
            dispatcher.execute(batchable(() -> calls.add("second")));
        }));

        assertEquals(1, executor.size());
        executor.runNext();
        assertEquals(List.of("outer", "first", "second"), calls);
        assertEquals(0, executor.size());
        assertEquals(0, dispatcher.inhabitants());
        dispatcher.close();
    }

    @Test
    void nonBatchableTasksBypassTheCurrentBatch() {
        system = ActorSystem.create("batching-unbatched");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        List<String> calls = new ArrayList<>();

        dispatcher.execute(batchable(() -> {
            calls.add("outer");
            dispatcher.execute(() -> calls.add("plain"));
            dispatcher.execute(new Batchable() {
                @Override
                public boolean isBatchable() {
                    return false;
                }

                @Override
                public void run() {
                    calls.add("disabled");
                }
            });
        }));

        executor.runNext();
        assertEquals(List.of("outer"), calls);
        assertEquals(2, executor.size());
        executor.runNext();
        executor.runNext();
        assertEquals(List.of("outer", "plain", "disabled"), calls);
        dispatcher.close();
    }

    @Test
    void failureResubmitsRemainingTasksAndRestoresBlockContext() {
        system = ActorSystem.create("batching-failure");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new DispatcherLifecycleTest.CaptureActorRef(events), Error.class);
        List<String> calls = new ArrayList<>();
        RuntimeException failure = new IllegalStateException("batch failed");

        dispatcher.execute(batchable(() -> {
            calls.add("first");
            dispatcher.execute(batchable(() -> calls.add("remaining")));
            throw failure;
        }));

        executor.runNext();
        assertEquals(List.of("first"), calls);
        assertEquals(1, executor.size());
        assertSame(BlockContext.DEFAULT, BlockContext.current());
        Error event = assertInstanceOf(Error.class, events.poll());
        assertNotNull(event);
        assertSame(failure, event.cause());

        executor.runNext();
        assertEquals(List.of("first", "remaining"), calls);
        assertSame(BlockContext.DEFAULT, BlockContext.current());
        dispatcher.close();
    }

    @Test
    void blockOnResubmitsTheRemainingBatch() {
        system = ActorSystem.create("batching-block-on");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        AtomicBoolean remainingRan = new AtomicBoolean();
        AtomicInteger queuedDuringBlock = new AtomicInteger(-1);

        dispatcher.execute(batchable(() -> {
            dispatcher.execute(batchable(() -> remainingRan.set(true)));
            BlockContext.current().blockOn(() -> {
                queuedDuringBlock.set(executor.size());
                executor.runNext();
                return null;
            }, new CanAwait() {});
        }));

        executor.runNext();
        assertEquals(1, queuedDuringBlock.get());
        assertTrue(remainingRan.get());
        assertEquals(0, executor.size());
        assertSame(BlockContext.DEFAULT, BlockContext.current());
        dispatcher.close();
    }

    @Test
    void virtualizedExecutorDisablesBatching() {
        system = ActorSystem.create("batching-virtualized");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, true);
        List<String> calls = new ArrayList<>();

        dispatcher.execute(batchable(() -> {
            calls.add("outer");
            dispatcher.execute(batchable(() -> calls.add("inner")));
        }));

        assertEquals(1, executor.size());
        executor.runNext();
        assertEquals(List.of("outer"), calls);
        assertEquals(1, executor.size());
        executor.runNext();
        assertEquals(List.of("outer", "inner"), calls);
        dispatcher.close();
    }

    @Test
    void logEventExceptionIsPublishedWithoutRewrapping() {
        system = ActorSystem.create("batching-log-event");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new DispatcherLifecycleTest.CaptureActorRef(events), Error.class);
        Error error = new Error("original", Dispatcher.class, "already structured");

        dispatcher.reportFailure(new LogEventException(error, new IllegalStateException("cause")));

        assertSame(error, events.poll());
        dispatcher.close();
    }

    @Test
    void dispatcherRetainsUpstreamIdentityAndMailboxMarker() {
        system = ActorSystem.create("batching-identity");
        QueueExecutor executor = new QueueExecutor();
        Dispatcher dispatcher = dispatcher(executor, false);
        ActorCell actor = new ActorCell((ActorSystemImpl) system, null, null, dispatcher, null);

        assertEquals("test-dispatcher", dispatcher.id());
        assertEquals("Dispatcher[test-dispatcher]", dispatcher.toString());
        assertSame(executor, dispatcher.executorService().executor());
        Mailbox mailbox = dispatcher.createMailbox(actor,
                (owner, actorSystem) -> new UnboundedMailbox.MessageQueue());
        assertInstanceOf(DefaultSystemMessageQueue.class, mailbox);
        dispatcher.close();
    }

    private Dispatcher dispatcher(QueueExecutor executor, boolean virtualized) {
        ExecutorServiceFactoryProvider provider = new ExecutorServiceFactoryProvider() {
            @Override
            public ExecutorServiceFactory createExecutorServiceFactory(String id, java.util.concurrent.ThreadFactory threadFactory) {
                return () -> executor;
            }

            @Override
            public boolean isVirtualized() {
                return virtualized;
            }
        };
        return new Dispatcher(DispatcherTestSupport.configurator(system, system.scheduler()), "test-dispatcher",
                5, Duration.ofMillis(100), provider, Duration.ofHours(1));
    }

    private static Batchable batchable(Runnable runnable) {
        return new Batchable() {
            @Override
            public boolean isBatchable() {
                return true;
            }

            @Override
            public void run() {
                runnable.run();
            }
        };
    }

    private static final class QueueExecutor extends AbstractExecutorService {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean shutdown;

        @Override
        public void execute(Runnable command) {
            if (shutdown) throw new java.util.concurrent.RejectedExecutionException("shutdown");
            tasks.add(command);
        }

        void runNext() {
            Runnable task = tasks.removeFirst();
            task.run();
        }

        int size() {
            return tasks.size();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown();
            List<Runnable> remaining = new ArrayList<>(tasks);
            tasks.clear();
            return remaining;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }
}
