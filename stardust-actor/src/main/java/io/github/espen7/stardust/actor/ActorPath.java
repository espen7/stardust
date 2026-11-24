package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * Actor 的逻辑地址。路径相等只比较地址文本，ActorRef identity 还会比较 UID。
 */
public final class ActorPath implements Comparable<ActorPath>, Serializable {

    public static final int UndefinedUid = 0;
    private final String value;
    private final int uid;

    public ActorPath(String value) {
        this(value, UndefinedUid);
    }

    public ActorPath(String value, int uid) {
        this.value = value;
        this.uid = uid;
    }

    /**
     * 返回 Actor incarnation UID。
     */
    public int uid() {
        return uid;
    }

    /**
     * 返回路径最后一个元素，对应 Pekko ActorPath.name。
     */
    public String name() {
        int separator = value.lastIndexOf('/');
        return value.substring(separator + 1);
    }

    public String toStringWithoutAddress() {
        int scheme = value.indexOf("://");
        if (scheme < 0) return value;
        int firstElement = value.indexOf('/', scheme + 3);
        return firstElement < 0 ? "/" : value.substring(firstElement);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(ActorPath other) {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ActorPath path && value.equals(path.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
