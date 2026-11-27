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
package io.github.espen7.tapio.actor;

import java.util.List;

/** 支持通过关联 cell 查询父级、子引用以及创建配置的本地引用。 */
public abstract class ActorRefWithCell extends InternalActorRef {
    /** @return 当前用于投递的 cell，外部挂载时可能仍在等待激活 */
    public abstract Cell underlying();

    abstract ActorSystemImpl system();

    public abstract InternalActorRef getParent();

    abstract InternalActorRef getSingleChild(String name);

    abstract List<ActorRef> children();

    abstract Props props();
}
