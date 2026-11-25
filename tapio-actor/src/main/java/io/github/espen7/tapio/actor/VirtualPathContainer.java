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

import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider 用于保存已经注册的临时引用的容器。
 */
final class VirtualPathContainer extends MinimalActorRef {
    private final ActorRefProvider provider;
    private final ActorPath path;
    private final LoggingAdapter log;
    private final ConcurrentHashMap<String, InternalActorRef> children = new ConcurrentHashMap<>();

    VirtualPathContainer(ActorRefProvider provider, ActorPath path, LoggingAdapter log) {
        this.provider = provider;
        this.path = path;
        this.log = log;
    }

    @Override
    public ActorRefProvider provider() {
        return provider;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    void addChild(String name, InternalActorRef ref) {
        InternalActorRef old = children.put(name, ref);
        if (old != null) {
            log.debug("{} replacing child {} ({} -> {})", path, name, old, ref);
            old.stop();
        }
    }

    void removeChild(String name) {
        if (children.remove(name) == null) log.warning("{} trying to remove non-child {}", path, name);
    }

    InternalActorRef getChild(String name) {
        return children.get(name);
    }
}
