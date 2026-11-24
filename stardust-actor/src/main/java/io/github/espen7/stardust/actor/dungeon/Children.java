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
package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.actor.Props;

/**
 * Pekko dungeon.Children 的 Java 对照入口。
 *
 * <p>Scala trait 在 ActorCell 中通过混入共享状态和方法。Java 版本无法采用相同
 * 的 trait composition，因此把入口保留在同名类中，并委托给 ActorCell 管理状态。</p>
 */
public final class Children {
    private Children() {
    }

    /**
     * 通过 ActorCell 的 ChildrenContainer 预留 name 并创建 child。
     *
     * @param cell 当前 ActorCell
     * @param props child 的创建配置
     * @param name child name
     * @return 新 child 的 ActorRef
     */
    public static ActorRef actorOf(ActorCell cell, Props props, String name) {
        return cell.actorOf(props, name);
    }

    /**
     * 通过 ActorRef 发起异步停止。
     *
     * @param cell 当前 ActorCell
     * @param ref 要停止的 child
     */
    public static void stop(ActorCell cell, ActorRef ref) {
        cell.stop(ref);
    }
}
