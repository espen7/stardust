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
import java.util.PriorityQueue;

/**
 * 对相同优先级元素保持 FIFO 顺序的非阻塞优先级队列。
 *
 * @param <E> 元素类型
 */
public class StablePriorityQueue<E> extends PriorityQueueStabilizer<E> {
    private final PriorityQueue<WrappedElement<E>> backingQueue;

    /**
     * 创建稳定优先级队列。
     *
     * @param capacity 初始容量（必须大于 0）
     * @param cmp 优先级比较器
     */
    public StablePriorityQueue(int capacity, Comparator<E> cmp) {
        this.backingQueue = new PriorityQueue<>(capacity, new WrappedElementComparator<>(cmp));
    }

    @Override
    protected AbstractQueue<WrappedElement<E>> backingQueue() {
        return backingQueue;
    }
}
