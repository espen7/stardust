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

/**
 * Pekko dungeon.FaultHandling 的 Java 对照入口。
 *
 * <p>失败处理由 ActorCell 的当前 supervisor strategy 决定。restart 和 terminate
 * 都作为系统消息排队，避免在 receive 回调尚未退出时重入生命周期钩子。</p>
 */
public final class FaultHandling {
    private FaultHandling() {
    }

    /**
     * 请求 ActorCell 重新创建 Actor 实例。
     *
     * @param cell 当前 ActorCell
     * @param cause restart 原因
     */
    public static void restart(ActorCell cell, Throwable cause) {
        cell.sendSystemMessage(new io.github.espen7.stardust.dispatch.sysmsg.SystemMessage.Recreate(cause));
    }

    /**
     * 请求 ActorCell 完成终止流程。
     *
     * @param cell 当前 ActorCell
     */
    public static void terminate(ActorCell cell) {
        cell.sendSystemMessage(new io.github.espen7.stardust.dispatch.sysmsg.SystemMessage.Terminate());
    }
}
