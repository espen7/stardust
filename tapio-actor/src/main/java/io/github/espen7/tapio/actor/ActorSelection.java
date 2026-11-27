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

import io.github.espen7.tapio.pattern.Patterns;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** 从锚点匹配 Actor 路径并投递消息。每次投递都会查询当时的 Actor tree。 */
public final class ActorSelection implements Serializable {
    private final InternalActorRef anchor;
    private final List<SelectionPathElement> path;

    ActorSelection(InternalActorRef anchor, Iterable<String> elements) {
        this.anchor = anchor;
        List<SelectionPathElement> compiled = new ArrayList<>();
        for (String element : elements) {
            if (element.isEmpty()) continue;
            if (element.indexOf('?') >= 0 || element.indexOf('*') >= 0) {
                compiled.add(new SelectChildPattern(element));
            } else if (element.equals("..")) {
                compiled.add(SelectParent.INSTANCE);
            } else {
                compiled.add(new SelectChildName(element));
            }
        }
        this.path = List.copyOf(compiled);
    }

    /**
     * 从显式锚点和相对路径创建选择。
     *
     * @param anchor 起始引用，须由 ActorSystem 创建
     * @param path 相对路径，可含星号、问号和父级元素
     * @return 从指定引用开始的选择
     */
    public static ActorSelection apply(ActorRef anchor, String path) {
        return new ActorSelection((InternalActorRef) anchor, Arrays.asList(path.split("/+")));
    }

    static ActorSelection apply(ActorRefProvider provider, InternalActorRef lookupRoot, String path) {
        try {
            URI uri = new URI(path);
            String rawPath = uri.getRawPath();
            if (rawPath != null) {
                if (uri.getRawFragment() != null) rawPath += "#" + uri.getRawFragment();
                List<String> elements = Arrays.asList(rawPath.split("/", -1));
                if (!uri.isAbsolute()) {
                    return rawPath.startsWith("/")
                            ? new ActorSelection(provider.rootGuardian(), elements.subList(1, elements.size()))
                            : new ActorSelection(lookupRoot, elements);
                }
                Address address = ActorPath.address(uri);
                if (address != null) {
                    return new ActorSelection((InternalActorRef) provider.rootGuardianAt(address),
                            elements.subList(1, elements.size()));
                }
            }
        } catch (URISyntaxException invalidPath) {
            return new ActorSelection((InternalActorRef) provider.deadLetters(), List.of());
        }
        return new ActorSelection((InternalActorRef) provider.deadLetters(), List.of());
    }

    static ActorSelection apply(ActorRefProvider provider, ActorPath path) {
        return new ActorSelection((InternalActorRef) provider.rootGuardianAt(path.address()), path.elements());
    }

    /** @return 投递从此引用开始，供 ask 获取锚点的 Provider */
    public ActorRef anchor() {
        return anchor;
    }

    /** @return 此选择的锚点路径 */
    public ActorPath anchorPath() {
        return anchor.path();
    }

    /** @return 以斜杠开头的选择路径，保留通配符 */
    public String pathString() {
        return "/" + String.join("/", path.stream().map(Object::toString).toList());
    }

    /** @return 带锚点地址的选择文本，用于重建选择 */
    public String toSerializationFormat() {
        String anchorPath = anchor.path().toString();
        if (path.isEmpty()) return anchorPath;
        return anchorPath.endsWith("/") ? anchorPath + pathString().substring(1) : anchorPath + pathString();
    }

    /**
     * 向当前匹配的 Actor 异步投递消息。通配符可以匹配多个 Actor。
     *
     * @param message 要投递的非空消息
     * @param sender 发件人，可以为 {@link ActorRef#noSender}
     */
    public void tell(Object message, ActorRef sender) {
        deliverSelection(anchor, sender, new ActorSelectionMessage(message, path, false));
    }

    /**
     * 保留当前消息的 sender 并转发。应从处理该消息的 Actor 调用。
     *
     * @param message 要转发的消息
     * @param context 当前 Actor 的上下文
     */
    public void forward(Object message, ActorContext context) {
        tell(message, context.sender());
    }

    /**
     * 通过 Identify 异步获取首个匹配的引用。
     *
     * @param timeout 等待回复的时限
     * @return 解析结果，未找到或超时时以 {@link ActorNotFound} 失败
     */
    public CompletionStage<ActorRef> resolveOne(Duration timeout) {
        return Patterns.ask(this, new Identify(null), timeout).handle((reply, failure) -> {
            if (failure == null && reply instanceof ActorIdentity identity && identity.ref().isPresent()) {
                return identity.ref().orElseThrow();
            }
            throw new ActorNotFound(this);
        });
    }

    static void deliverSelection(InternalActorRef anchor, ActorRef sender, ActorSelectionMessage selection) {
        List<SelectionPathElement> elements = selection.elements();
        if (elements.isEmpty()) {
            anchor.tell(selection.message(), sender);
            return;
        }
        InternalActorRef ref = anchor;
        for (int index = 0; index < elements.size(); index++) {
            if (!(ref instanceof ActorRefWithCell withCell)) {
                ref.tell(new ActorSelectionMessage(selection.message(), elements.subList(index, elements.size()),
                        selection.wildcardFanOut()), sender);
                return;
            }
            SelectionPathElement element = elements.get(index);
            boolean last = index + 1 == elements.size();
            if (element instanceof SelectChildPattern pattern) {
                List<ActorRef> matching = withCell.children().stream()
                        .filter(child -> pattern.matches(child.path().name())).toList();
                if (matching.isEmpty()) {
                    deliverToEmptyRef(anchor, withCell, sender, selection);
                } else if (last) {
                    matching.forEach(child -> child.tell(selection.message(), sender));
                } else {
                    var remainder = new ActorSelectionMessage(selection.message(),
                            elements.subList(index + 1, elements.size()),
                            selection.wildcardFanOut() || matching.size() > 1);
                    matching.forEach(child -> deliverSelection((InternalActorRef) child, sender, remainder));
                }
                return;
            }
            ref = element == SelectParent.INSTANCE ? withCell.getParent()
                    : withCell.getSingleChild(((SelectChildName) element).name());
            if (ref == null || ref == Nobody.INSTANCE) {
                deliverToEmptyRef(anchor, withCell, sender, selection);
                return;
            }
            if (last) ref.tell(selection.message(), sender);
        }
    }

    private static void deliverToEmptyRef(InternalActorRef anchor, ActorRefWithCell owner, ActorRef sender,
                                          ActorSelectionMessage selection) {
        if (!selection.wildcardFanOut()) {
            ActorPath path = anchor.path();
            for (SelectionPathElement element : selection.elements()) {
                path = path.child(element.toString());
            }
            new EmptyLocalActorRef(owner.provider(), path, owner.system().eventStream()).tell(selection, sender);
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ActorSelection selection && anchor.equals(selection.anchor)
                && path.equals(selection.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(anchor, path);
    }

    @Override
    public String toString() {
        ActorPath anchorPath = anchor.path();
        String uid = anchorPath.uid() == ActorPath.UndefinedUid ? "" : "#" + anchorPath.uid();
        return "ActorSelection[Anchor(" + anchorPath + uid + "), Path(" + pathString() + ")]";
    }
}
