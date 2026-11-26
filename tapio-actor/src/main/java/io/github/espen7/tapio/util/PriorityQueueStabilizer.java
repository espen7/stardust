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
package io.github.espen7.tapio.util;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 包装底层优先级队列以保证相同优先级元素按 FIFO 顺序出队，对应 Pekko {@code PriorityQueueStabilizer}。
 *
 * @param <E> 队列元素类型
 */
public abstract class PriorityQueueStabilizer<E> extends AbstractQueue<E> {
    /** 包装原始元素与递增序号的内部节点。 */
    public static final class WrappedElement<E> {
        final E element;
        final long seqNum;

        WrappedElement(E element, long seqNum) {
            this.element = element;
            this.seqNum = seqNum;
        }
    }

    /** 先比较原始优先级，相等时再比较递增序号的比较器。 */
    public static final class WrappedElementComparator<E> implements Comparator<WrappedElement<E>> {
        private final Comparator<E> cmp;

        public WrappedElementComparator(Comparator<E> cmp) {
            this.cmp = Objects.requireNonNull(cmp, "cmp must not be null");
        }

        @Override
        public int compare(WrappedElement<E> e1, WrappedElement<E> e2) {
            int baseComparison = cmp.compare(e1.element, e2.element);
            if (baseComparison != 0) {
                return baseComparison;
            }
            return Long.signum(e1.seqNum - e2.seqNum);
        }
    }

    protected final AtomicLong seqNum = new AtomicLong(0);

    /** 创建 PriorityQueueStabilizer 实例。 */
    protected PriorityQueueStabilizer() {
    }

    protected abstract AbstractQueue<WrappedElement<E>> backingQueue();

    @Override
    public E peek() {
        WrappedElement<E> wrapped = backingQueue().peek();
        return wrapped == null ? null : wrapped.element;
    }

    @Override
    public int size() {
        return backingQueue().size();
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        return backingQueue().offer(new WrappedElement<>(e, seqNum.incrementAndGet()));
    }

    @Override
    public Iterator<E> iterator() {
        Iterator<WrappedElement<E>> backingIterator = backingQueue().iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return backingIterator.hasNext();
            }

            @Override
            public E next() {
                return backingIterator.next().element;
            }

            @Override
            public void remove() {
                backingIterator.remove();
            }
        };
    }

    @Override
    public E poll() {
        WrappedElement<E> wrapped = backingQueue().poll();
        return wrapped == null ? null : wrapped.element;
    }
}
