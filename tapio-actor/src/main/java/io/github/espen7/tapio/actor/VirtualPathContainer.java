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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider 用于保存已经注册的临时引用的容器。
 */
final class VirtualPathContainer extends MinimalActorRef {
    private final ActorRefProvider provider;
    private final ActorPath path;
    private final InternalActorRef parent;
    private final LoggingAdapter log;
    private final ConcurrentHashMap<String, InternalActorRef> children = new ConcurrentHashMap<>();

    VirtualPathContainer(ActorRefProvider provider, ActorPath path, InternalActorRef parent, LoggingAdapter log) {
        this.provider = provider;
        this.path = path;
        this.parent = parent;
        this.log = log;
    }

    InternalActorRef getParent() {
        return parent;
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
            ((InternalActorRef) old).stop();
        }
    }

    void removeChild(String name) {
        if (children.remove(name) == null) log.warning("{} trying to remove non-child {}", path, name);
    }

    InternalActorRef getChild(String name) {
        return children.get(name);
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        if (!(message instanceof ActorSelectionMessage selection)) {
            super.tell(message, sender);
            return;
        }
        if (selection.elements().isEmpty()) throw new IllegalArgumentException("requirement failed");
        SelectionPathElement first = selection.elements().getFirst();
        InternalActorRef child = first instanceof SelectChildName selected ? getChild(selected.name()) : null;
        if (child != null && selection.elements().size() == 1) {
            child.tell(selection.message(), sender);
        } else if (!selection.wildcardFanOut()) {
            ActorPath emptyPath = path;
            for (SelectionPathElement element : selection.elements()) {
                emptyPath = emptyPath.child(element.toString());
            }
            new EmptyLocalActorRef(provider, emptyPath, provider.systemGuardian().system().eventStream())
                    .tell(selection.message(), sender);
        }
    }

    @Override
    InternalActorRef getChild(Iterator<String> names) {
        if (!names.hasNext()) return this;
        String name = names.next();
        if (name.isEmpty()) return this;
        InternalActorRef child = getChild(name);
        if (child == null) return Nobody.INSTANCE;
        return !names.hasNext() ? child : child.getChild(names);
    }
}
