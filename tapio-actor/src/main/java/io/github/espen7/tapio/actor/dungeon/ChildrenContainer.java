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
package io.github.espen7.tapio.actor.dungeon;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.InvalidActorNameException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * 管理 ActorCell children 集合及其生命周期状态的接口。
 *
 * <p>实现通过不可变状态转换表达 child 注册、停止等待、重建等待和终止状态。</p>
 */
public interface ChildrenContainer {
    /** 表示 children 状态转换期间 ActorCell 的暂停原因。 */
    interface SuspendReason {
    }

    /** 表示由用户请求引起的 child 停止等待状态。 */
    enum UserRequest implements SuspendReason {
        /** 用户请求实例。 */
        INSTANCE
    }

    /**
     * 表示 ActorCell 正在等待停止 children 后重建自身。
     *
     * @param cause restart 原因
     */
    record Recreation(Throwable cause) implements SuspendReason, WaitingForChildren {
    }

    /** 表示 ActorCell 正在等待停止 children 后完成创建。 */
    record Creation() implements SuspendReason, WaitingForChildren {
    }

    /** 表示 ActorCell 正在等待 children 全部终止。 */
    enum Termination implements SuspendReason {
        /** 终止实例。 */
        INSTANCE
    }

    /** 标记正在等待至少一个 child 终止的容器。 */
    interface WaitingForChildren {
    }

    /** 无 children 的初始容器。 */
    ChildrenContainer EMPTY = new EmptyChildrenContainer();

    /** 所有 children 终止后安装的容器。 */
    ChildrenContainer TERMINATED = new TerminatedChildrenContainer();

    /**
     * 返回共享的空 children 容器。
     *
     * @return 空容器
     */
    static ChildrenContainer empty() {
        return EMPTY;
    }

    /**
     * 返回共享的已终止 children 容器。
     *
     * @return 已终止容器
     */
    static ChildrenContainer terminated() {
        return TERMINATED;
    }

    /**
     * 注册指定名称的 child。
     *
     * @param name child 名称
     * @param stats child 重启统计
     * @return 更新后的容器
     */
    ChildrenContainer add(String name, ChildRestartStats stats);

    /**
     * 移除指定 child。
     *
     * @param child 要移除的 child
     * @return 更新后的容器
     */
    ChildrenContainer remove(ActorRef child);

    /**
     * 按名称查找 child 状态。
     *
     * @param name child 名称
     * @return child 统计或名称预留标记，不存在时返回 {@code null}
     */
    ChildStats getByName(String name);

    /**
     * 按 ActorRef 查找 child 重启统计，并校验 incarnation。
     *
     * @param actor child 引用
     * @return 匹配的统计，不存在时返回 {@code null}
     */
    ChildRestartStats getByRef(ActorRef actor);

    /**
     * 返回按名称排序的已注册 child。
     *
     * @return child 引用快照
     */
    List<ActorRef> children();

    /**
     * 返回按名称排序的 child 重启统计。
     *
     * @return child 统计快照
     */
    List<ChildRestartStats> stats();

    /**
     * 将 child 标记为等待终止。
     *
     * @param actor 要停止的 child
     * @return 更新后的容器
     */
    ChildrenContainer shallDie(ActorRef actor);

    /**
     * 预留 child 名称。
     *
     * @param name 要预留的名称
     * @return 更新后的容器
     * @throws IllegalArgumentException 名称已被占用
     * @throws IllegalStateException 容器已终止或正在停止
     */
    ChildrenContainer reserve(String name);

    /**
     * 取消尚未完成创建的名称预留。
     *
     * @param name 要取消预留的名称
     * @return 更新后的容器
     */
    ChildrenContainer unreserve(String name);

    /**
     * 判断容器是否处于终止状态。
     *
     * @return 正在终止或已经终止时为 {@code true}
     */
    boolean isTerminating();

    /**
     * 判断容器是否处于正常状态。
     *
     * @return 无生命周期等待或仅等待用户停止时为 {@code true}
     */
    boolean isNormal();

    /** 表示没有已注册 children 的状态。 */
    class EmptyChildrenContainer implements ChildrenContainer {
        /** 创建空 children 容器。 */
        public EmptyChildrenContainer() {
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer add(String name, ChildRestartStats stats) {
            TreeMap<String, ChildStats> children = new TreeMap<>();
            children.put(name, stats);
            return new NormalChildrenContainer(children);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer remove(ActorRef child) {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ChildStats getByName(String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public ChildRestartStats getByRef(ActorRef actor) {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public List<ActorRef> children() {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override
        public List<ChildRestartStats> stats() {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer shallDie(ActorRef actor) {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer reserve(String name) {
            TreeMap<String, ChildStats> children = new TreeMap<>();
            children.put(name, ChildNameReserved.INSTANCE);
            return new NormalChildrenContainer(children);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer unreserve(String name) {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isTerminating() {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isNormal() {
            return true;
        }
    }

    /** 表示停止流程完成后的终止状态。 */
    final class TerminatedChildrenContainer extends EmptyChildrenContainer {
        /** 创建已终止 children 容器。 */
        public TerminatedChildrenContainer() {
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer add(String name, ChildRestartStats stats) {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer reserve(String name) {
            throw new IllegalStateException("cannot reserve actor name '" + name + "': already terminated");
        }

        /** {@inheritDoc} */
        @Override
        public boolean isTerminating() {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isNormal() {
            return false;
        }
    }

    /** 表示存在 children 且没有等待 child 终止的状态。 */
    final class NormalChildrenContainer implements ChildrenContainer {
        private final TreeMap<String, ChildStats> children;

        /**
         * 创建普通容器，空 map 时返回共享空容器。
         *
         * @param children 按名称索引的 child 状态
         * @return 对应状态的容器
         */
        public static ChildrenContainer apply(TreeMap<String, ChildStats> children) {
            return children.isEmpty() ? ChildrenContainer.empty() : new NormalChildrenContainer(children);
        }

        NormalChildrenContainer(TreeMap<String, ChildStats> children) {
            this.children = new TreeMap<>(children);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer add(String name, ChildRestartStats stats) {
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.put(name, stats);
            return new NormalChildrenContainer(updated);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer remove(ActorRef child) {
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.remove(child.path().name());
            return apply(updated);
        }

        /** {@inheritDoc} */
        @Override
        public ChildStats getByName(String name) {
            return children.get(name);
        }

        /** {@inheritDoc} */
        @Override
        public ChildRestartStats getByRef(ActorRef actor) {
            ChildStats stats = children.get(actor.path().name());
            return stats instanceof ChildRestartStats child && child.child().equals(actor) ? child : null;
        }

        /** {@inheritDoc} */
        @Override
        public List<ActorRef> children() {
            List<ActorRef> result = new ArrayList<>();
            for (ChildStats stats : children.values()) {
                if (stats instanceof ChildRestartStats child) result.add(child.child());
            }
            return List.copyOf(result);
        }

        /** {@inheritDoc} */
        @Override
        public List<ChildRestartStats> stats() {
            List<ChildRestartStats> result = new ArrayList<>();
            for (ChildStats stats : children.values()) {
                if (stats instanceof ChildRestartStats child) result.add(child);
            }
            return List.copyOf(result);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer shallDie(ActorRef actor) {
            return new TerminatingChildrenContainer(children, Set.of(actor), UserRequest.INSTANCE);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer reserve(String name) {
            if (children.containsKey(name)) {
                throw new InvalidActorNameException("actor name [" + name + "] is not unique!");
            }
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.put(name, ChildNameReserved.INSTANCE);
            return new NormalChildrenContainer(updated);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer unreserve(String name) {
            if (children.get(name) != ChildNameReserved.INSTANCE) return this;
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.remove(name);
            return apply(updated);
        }

        /** {@inheritDoc} */
        @Override
        public boolean isTerminating() {
            return false;
        }

        @Override
        public boolean isNormal() {
            return true;
        }
    }

    /** 表示至少有一个 child 已收到停止请求、尚未完成终止的状态。 */
    final class TerminatingChildrenContainer implements ChildrenContainer {
        private final TreeMap<String, ChildStats> children;
        private final Set<ActorRef> toDie;
        private final SuspendReason reason;

        TerminatingChildrenContainer(TreeMap<String, ChildStats> children, Set<ActorRef> toDie, SuspendReason reason) {
            this.children = new TreeMap<>(children);
            this.toDie = Set.copyOf(toDie);
            this.reason = reason;
        }

        /**
         * 返回等待 child 终止期间保存的生命周期原因。
         *
         * @return 当前暂停原因
         */
        public SuspendReason reason() {
            return reason;
        }

        /**
         * 判断指定 child 是否属于待终止集合。
         *
         * @param child child 引用
         * @return child 正在终止时为 {@code true}
         */
        public boolean isWaitingFor(ActorRef child) {
            return toDie.contains(child);
        }

        /**
         * 判断容器是否仍在等待 child 终止。
         *
         * @return 尚有待终止 child 时为 {@code true}
         */
        public boolean isTerminatingChildren() {
            return !toDie.isEmpty();
        }

        /**
         * 更新等待 child 终止期间保存的生命周期原因。
         *
         * @param reason 新的暂停原因
         * @return 更新后的容器
         */
        public ChildrenContainer withReason(SuspendReason reason) {
            return new TerminatingChildrenContainer(children, toDie, reason);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer add(String name, ChildRestartStats stats) {
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.put(name, stats);
            return new TerminatingChildrenContainer(updated, toDie, reason);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer remove(ActorRef child) {
            Set<ActorRef> remainingToDie = new HashSet<>(toDie);
            remainingToDie.remove(child);
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.remove(child.path().name());
            if (remainingToDie.isEmpty()) {
                return reason == Termination.INSTANCE
                        ? ChildrenContainer.terminated()
                        : NormalChildrenContainer.apply(updated);
            }
            return new TerminatingChildrenContainer(updated, remainingToDie, reason);
        }

        /** {@inheritDoc} */
        @Override
        public ChildStats getByName(String name) {
            return children.get(name);
        }

        /** {@inheritDoc} */
        @Override
        public ChildRestartStats getByRef(ActorRef actor) {
            ChildStats stats = children.get(actor.path().name());
            return stats instanceof ChildRestartStats child && child.child().equals(actor) ? child : null;
        }

        /** {@inheritDoc} */
        @Override
        public List<ActorRef> children() {
            List<ActorRef> result = new ArrayList<>();
            for (ChildStats stats : children.values()) {
                if (stats instanceof ChildRestartStats child) result.add(child.child());
            }
            return List.copyOf(result);
        }

        /** {@inheritDoc} */
        @Override
        public List<ChildRestartStats> stats() {
            List<ChildRestartStats> result = new ArrayList<>();
            for (ChildStats stats : children.values()) {
                if (stats instanceof ChildRestartStats child) result.add(child);
            }
            return List.copyOf(result);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer shallDie(ActorRef actor) {
            Set<ActorRef> updated = new HashSet<>(toDie);
            updated.add(actor);
            return new TerminatingChildrenContainer(children, updated, reason);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer reserve(String name) {
            if (reason == Termination.INSTANCE) {
                throw new IllegalStateException("cannot reserve actor name '" + name + "': terminating");
            }
            if (children.containsKey(name)) {
                throw new InvalidActorNameException("actor name [" + name + "] is not unique!");
            }
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.put(name, ChildNameReserved.INSTANCE);
            return new TerminatingChildrenContainer(updated, toDie, reason);
        }

        /** {@inheritDoc} */
        @Override
        public ChildrenContainer unreserve(String name) {
            if (children.get(name) != ChildNameReserved.INSTANCE) return this;
            TreeMap<String, ChildStats> updated = new TreeMap<>(children);
            updated.remove(name);
            return new TerminatingChildrenContainer(updated, toDie, reason);
        }

        /** {@inheritDoc} */
        @Override
        public boolean isTerminating() {
            return reason == Termination.INSTANCE;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isNormal() {
            return reason == UserRequest.INSTANCE;
        }
    }
}
