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
package io.github.espen7.tapio.dispatch.sysmsg;

import io.github.espen7.tapio.actor.PossiblyHarmful;
import io.github.espen7.tapio.annotation.InternalStableApi;

import java.io.Serializable;

/**
 * Actor runtime 内部控制消息的基类。
 *
 * <p>next 字段让 SystemMessage 自身成为链表节点，保持 Pekko 中避免额外包装对象
 * 的设计。每个消息只能进入一个 Mailbox，处理前会调用 unlink。</p>
 */
@InternalStableApi
public abstract sealed class SystemMessage implements PossiblyHarmful, Serializable
        permits Create, Recreate, Suspend, Resume, Terminate, Failed, Supervise, Watch, Unwatch,
        DeathWatchNotification, NoMessage {
    private transient SystemMessage next;

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

}
