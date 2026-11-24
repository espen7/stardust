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
package io.github.espen7.stardust.dispatch.sysmsg;

import io.github.espen7.stardust.actor.ActorRef;

/**
 * Actor runtime 内部控制消息的基类。
 *
 * <p>next 字段让 SystemMessage 自身成为链表节点，保持 Pekko 中避免额外包装对象
 * 的设计。每个消息只能进入一个 Mailbox，处理前会调用 unlink。</p>
 */
public abstract class SystemMessage {
    /**
     * 标记系统消息队列已关闭的哨兵。
     */
    public static final SystemMessage NoMessage = new SystemMessage() {};

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

    /** 请求 guardian 停止指定 child。 */
    public static final class StopChild extends SystemMessage {
        public final ActorRef child;

        public StopChild(ActorRef child) {
            this.child = child;
        }
    }

    /** system guardian 终止阶段使用的消息。 */
    public static final class RegisterTerminationHook extends SystemMessage {
        public final ActorRef hook;

        public RegisterTerminationHook() {
            this(null);
        }

        public RegisterTerminationHook(ActorRef hook) {
            this.hook = hook;
        }
    }

    /** system guardian 通知 termination hook 开始关闭。 */
    public static final class TerminationHook extends SystemMessage {
    }

    /** termination hook 完成关闭。 */
    public static final class TerminationHookDone extends SystemMessage {
    }


    /* child 完成终止后发送给 parent，以更新 ChildrenContainer。 */

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
