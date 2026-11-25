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

import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorRef;

/**
 * Pekko dungeon.DeathWatch 的 Java 对照入口。
 *
 * <p>watch 和 unwatch 都以 SystemMessage 发送给被观察对象，由对方的 ActorCell
 * 串行更新 watchedBy。观察已终止的对象时，运行时需要立即投递 Terminated。</p>
 */
public final class DeathWatch {
    private DeathWatch() {
    }

    /**
     * 开始观察 subject 的终止状态。
     *
     * @param cell 当前 ActorCell
     * @param subject 要观察的 ActorRef
     * @return 被观察的 ActorRef
     */
    public static ActorRef watch(ActorCell cell, ActorRef subject) {
        return cell.watch(subject);
    }

    /**
     * 取消之前注册的观察关系。
     *
     * @param cell 当前 ActorCell
     * @param subject 要解除观察的 ActorRef
     * @return 被解除观察的 ActorRef
     */
    public static ActorRef unwatch(ActorCell cell, ActorRef subject) {
        return cell.unwatch(subject);
    }
}
