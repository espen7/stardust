/*
 * Derived from Apache Pekko, licensed under the Apache License, Version 2.0.
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.espen7.tapio.dispatch;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 支持多生产者和单消费者的链式队列。
 *
 * <p>消费者可以将取出的节点交给另一个队列。
 *
 * @param <T> 队列元素类型
 */
public abstract class AbstractNodeQueue<T> extends AtomicReference<AbstractNodeQueue.Node<T>> {
    private static final VarHandle tailHandle = findHandle(AbstractNodeQueue.class, "tail");
    private volatile Node<T> tail;

    /**
     * 创建使用初始哨兵节点的空队列。
     */
    protected AbstractNodeQueue() {
        Node<T> sentinel = new Node<>(null);
        tail = sentinel;
        set(sentinel);
    }

    /**
     * 将元素加入队尾。允许多个生产者并发调用。
     *
     * @param value 非 {@code null} 的待入队元素
     */
    public final void add(T value) {
        Node<T> node = new Node<>(value);
        getAndSet(node).setNext(node);
    }

    /**
     * 将独立节点加入队尾。允许多个生产者并发调用。
     *
     * @param node 已脱离原队列且包含有效元素的节点
     */
    public final void addNode(Node<T> node) {
        node.setNext(null);
        getAndSet(node).setNext(node);
    }

    /**
     * 取出队首元素。仅由消费者线程调用。
     *
     * @return 队首元素，队列为空时为 {@code null}
     */
    public final T poll() {
        Node<T> node = pollNode();
        if (node == null) return null;
        T value = node.value;
        node.value = null;
        return value;
    }

    /**
     * 判断队列当前是否为空。
     *
     * @return 为空返回 {@code true}
     */
    public final boolean isEmpty() {
        return get() == tail;
    }

    /**
     * 统计当前队列中的元素数量（O(n) 遍历）。
     *
     * @return 当前元素数
     */
    @SuppressWarnings("unchecked")
    public final int count() {
        int count = 0;
        Node<T> current = (Node<T>) tailHandle.getAcquire(this);
        while (true) {
            Node<T> next = current.next();
            if (next == null) {
                break;
            }
            count++;
            current = next;
        }
        return count;
    }

    /**
     * 取出承载队首元素的节点。仅由消费者线程调用。
     *
     * @return 可重新入队的独立节点，队列为空时为 {@code null}
     */
    @SuppressWarnings("unchecked")
    public final Node<T> pollNode() {
        Node<T> current = (Node<T>) tailHandle.getAcquire(this);
        Node<T> next = current.next();
        if (next == null && get() != current) {
            /* 生产者已经交换队首，等待它发布链指针。 */
            do {
                Thread.onSpinWait();
                next = current.next();
            } while (next == null);
        }
        if (next == null) return null;
        current.value = next.value;
        next.value = null;
        tailHandle.setRelease(this, next);
        current.setNext(null);
        return current;
    }

    private static VarHandle findHandle(Class<?> type, String field) {
        try {
            return MethodHandles.lookup().findVarHandle(type, field, Node.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    /**
     * 在队列间传递元素的链表节点。
     *
     * @param <T> 节点存放的元素类型
     */
    public static final class Node<T> {
        private static final VarHandle nextHandle = findHandle(Node.class, "next");
        /**
         * 节点存放的元素。持有脱离队列的节点时可以读取或替换此值。
         */
        public T value;
        private volatile Node<T> next;

        private Node(T value) {
            this.value = value;
        }

        @SuppressWarnings("unchecked")
        private Node<T> next() {
            return (Node<T>) nextHandle.getAcquire(this);
        }

        private void setNext(Node<T> next) {
            nextHandle.setRelease(this, next);
        }
    }
}
