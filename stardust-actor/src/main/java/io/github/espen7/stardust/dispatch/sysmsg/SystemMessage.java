package io.github.espen7.stardust.dispatch.sysmsg;

import io.github.espen7.stardust.actor.ActorRef;

/**
 * Actor runtime 内部控制消息的基类。
 *
 * <p>next 字段让 SystemMessage 自身成为链表节点，保持 Pekko 中避免额外包装对象
 * 的设计。每个消息只能进入一个 Mailbox，处理前会调用 unlink。</p>
 */
public abstract class SystemMessage {
    private SystemMessage next;

    public SystemMessage next() {
        return next;
    }

    public void next(SystemMessage next) {
        this.next = next;
    }

    public void unlink() {
        next = null;
    }

    public boolean unlinked() {
        return next == null;
    }

    /**
     * 异步完成 Actor 初始化。
     */
    public static final class Create extends SystemMessage {
        public final Throwable failure;

        public Create(Throwable failure) {
            this.failure = failure;
        }
    }

    /**
     * 请求重新创建 Actor 实例，保留 ActorRef 和 ActorCell。
     */
    public static final class Recreate extends SystemMessage {
        public final Throwable cause;

        public Recreate(Throwable cause) {
            this.cause = cause;
        }
    }

    /**
     * 暂停或恢复普通消息处理。
     */
    public static final class Suspend extends SystemMessage {
    }

    public static final class Resume extends SystemMessage {
        public final Throwable causedByFailure;

        public Resume(Throwable causedByFailure) {
            this.causedByFailure = causedByFailure;
        }
    }

    public static final class Terminate extends SystemMessage {
    }


    // child 完成终止后发送给 parent，以更新 ChildrenContainer

    /**
     * child failure 发给 parent，由 parent 的 SupervisorStrategy 决定如何处理。
     */
    public static final class Failed extends SystemMessage {
        public final ActorRef child;
        public final Throwable cause;
        public final int uid;

        public Failed(ActorRef child, Throwable cause, int uid) {
            this.child = child;
            this.cause = cause;
            this.uid = uid;
        }
    }

    /** 通知 supervisor 新 child 已建立。 */
    public static final class Supervise extends SystemMessage {
        public final ActorRef child;
        public final boolean async;

        public Supervise(ActorRef child, boolean async) {
            this.child = child;
            this.async = async;
        }
    }

    /**
     * DeathWatch 建立、解除和终止通知。
     */
    public static final class Watch extends SystemMessage {
        public final ActorRef watchee;
        public final ActorRef watcher;

        public Watch(ActorRef watchee, ActorRef watcher) {
            this.watchee = watchee;
            this.watcher = watcher;
        }
    }

    public static final class Unwatch extends SystemMessage {
        public final ActorRef watchee;
        public final ActorRef watcher;

        public Unwatch(ActorRef watchee, ActorRef watcher) {
            this.watchee = watchee;
            this.watcher = watcher;
        }
    }

    public static final class DeathWatchNotification extends SystemMessage {
        public final ActorRef actor;
        public final boolean existenceConfirmed;
        public final boolean addressTerminated;

        public DeathWatchNotification(ActorRef actor, boolean existenceConfirmed, boolean addressTerminated) {
            this.actor = actor;
            this.existenceConfirmed = existenceConfirmed;
            this.addressTerminated = addressTerminated;
        }
    }
}
