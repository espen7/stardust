package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorRef;

import java.util.*;

/**
 * ActorCell children 状态的不可变快照。
 *
 * <p>每次变更都返回新容器，由 ActorCell 通过 VarHandle CAS 发布。这个结构对应
 * Pekko ChildrenContainer 的 name reservation、child lookup 和 terminating 状态。</p>
 */
public final class ChildrenContainer {
    public enum SuspendReason {UserRequest, Recreation, Creation, Termination}

    private static final ChildrenContainer EMPTY = new ChildrenContainer(Map.of(), Set.of(), null, false);
    private static final ChildrenContainer TERMINATED = new ChildrenContainer(Map.of(), Set.of(), SuspendReason.Termination, true);

    private final Map<String, ChildStats> childrenByName;
    private final Set<ActorRef> toDie;
    private final SuspendReason reason;
    private final boolean terminated;

    private ChildrenContainer(Map<String, ChildStats> childrenByName, Set<ActorRef> toDie,
                              SuspendReason reason, boolean terminated) {
        this.childrenByName = Collections.unmodifiableMap(new TreeMap<>(childrenByName));
        this.toDie = Set.copyOf(toDie);
        this.reason = reason;
        this.terminated = terminated;
    }

    public static ChildrenContainer empty() {
        return EMPTY;
    }

    public static ChildrenContainer terminated() {
        return TERMINATED;
    }

    /**
     * 预留 name；name 已存在或 ActorCell 正在终止时抛出异常。
     */
    public ChildrenContainer reserve(String name) {
        if (terminated || reason == SuspendReason.Termination) {
            throw new IllegalStateException("cannot reserve actor name '" + name + "': terminating");
        }
        if (childrenByName.containsKey(name)) {
            throw new IllegalArgumentException("actor name [" + name + "] is not unique!");
        }
        Map<String, ChildStats> updated = new TreeMap<>(childrenByName);
        updated.put(name, ChildNameReserved.INSTANCE);
        return copy(updated, toDie, reason, false);
    }

    /**
     * 用 child 引用替换已预留的名字。
     */
    public ChildrenContainer add(String name, ChildRestartStats stats) {
        if (terminated) return this;
        Map<String, ChildStats> updated = new TreeMap<>(childrenByName);
        updated.put(name, stats);
        return copy(updated, toDie, reason, false);
    }

    /**
     * 取消失败创建留下的 name reservation。
     */
    public ChildrenContainer unreserve(String name) {
        if (childrenByName.get(name) != ChildNameReserved.INSTANCE) return this;
        Map<String, ChildStats> updated = new TreeMap<>(childrenByName);
        updated.remove(name);
        return copy(updated, toDie, reason, false);
    }

    public ChildStats getByName(String name) {
        return childrenByName.get(name);
    }

    public ChildRestartStats getByRef(ActorRef actor) {
        ChildStats stats = childrenByName.get(actor.path().name());
        return stats instanceof ChildRestartStats child && child.child().equals(actor) ? child : null;
    }

    public List<ActorRef> children() {
        List<ActorRef> result = new ArrayList<>();
        for (ChildStats stats : childrenByName.values()) {
            if (stats instanceof ChildRestartStats child) result.add(child.child());
        }
        return List.copyOf(result);
    }

    /**
     * 标记一个 child 正在停止，以便停止期间拒绝创建新的 children。
     */
    public ChildrenContainer shallDie(ActorRef actor) {
        if (getByRef(actor) == null || toDie.contains(actor)) return this;
        return copy(childrenByName, union(toDie, actor), SuspendReason.UserRequest, false);
    }

    public ChildrenContainer remove(ActorRef actor) {
        String name = actor.path().name();
        Map<String, ChildStats> updated = new TreeMap<>(childrenByName);
        ChildStats current = updated.get(name);
        if (!(current instanceof ChildRestartStats child) || !child.child().equals(actor)) return this;
        updated.remove(name);
        Set<ActorRef> remaining = new java.util.HashSet<>(toDie);
        remaining.remove(actor);
        if (remaining.isEmpty() && reason == SuspendReason.Termination) return TERMINATED;
        return copy(updated, remaining, remaining.isEmpty() ? null : reason, false);
    }

    public ChildrenContainer terminating() {
        if (childrenByName.isEmpty()) return TERMINATED;
        Set<ActorRef> children = Set.copyOf(children());
        return copy(childrenByName, children, SuspendReason.Termination, false);
    }

    public boolean isTerminating() {
        return terminated || reason == SuspendReason.Termination;
    }

    public boolean isNormal() {
        return reason == null || reason == SuspendReason.UserRequest;
    }

    private static Set<ActorRef> union(Set<ActorRef> refs, ActorRef ref) {
        java.util.HashSet<ActorRef> copy = new java.util.HashSet<>(refs);
        copy.add(ref);
        return Set.copyOf(copy);
    }

    private static ChildrenContainer copy(Map<String, ChildStats> children, Set<ActorRef> toDie,
                                          SuspendReason reason, boolean terminated) {
        return children.isEmpty() && toDie.isEmpty() && reason == null && !terminated
                ? EMPTY : new ChildrenContainer(children, toDie, reason, terminated);
    }
}
