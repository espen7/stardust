/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.event;

import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.AddressTerminated;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * INTERNAL API：管理 {@link AddressTerminated} 订阅者并在远端节点失联时广播通知的扩展。
 */
@InternalApi
public final class AddressTerminatedTopic extends AbstractExtensionId<AddressTerminatedTopic.Topic>
        implements ExtensionIdProvider {

    /** 单例实例。 */
    public static final AddressTerminatedTopic INSTANCE = new AddressTerminatedTopic();

    private AddressTerminatedTopic() {
    }

    @Override
    public ExtensionId<Topic> lookup() {
        return INSTANCE;
    }

    @Override
    public Topic createExtension(ExtendedActorSystem system) {
        return new Topic();
    }

    /**
     * {@link AddressTerminated} 主题发布器扩展实例。
     */
    public static final class Topic implements Extension {
        private final AtomicReference<Set<ActorRef>> subscribers = new AtomicReference<>(Set.of());

        /**
         * 订阅地址终止通知。
         *
         * @param subscriber 订阅者 ActorRef
         */
        public void subscribe(ActorRef subscriber) {
            while (true) {
                Set<ActorRef> cur = subscribers.get();
                if (cur.contains(subscriber)) return;
                Set<ActorRef> next = new HashSet<>(cur);
                next.add(subscriber);
                if (subscribers.compareAndSet(cur, Collections.unmodifiableSet(next))) return;
            }
        }

        /**
         * 退订地址终止通知。
         *
         * @param subscriber 订阅者 ActorRef
         */
        public void unsubscribe(ActorRef subscriber) {
            while (true) {
                Set<ActorRef> cur = subscribers.get();
                if (!cur.contains(subscriber)) return;
                Set<ActorRef> next = new HashSet<>(cur);
                next.remove(subscriber);
                if (subscribers.compareAndSet(cur, Collections.unmodifiableSet(next))) return;
            }
        }

        /**
         * 向所有订阅者发布 {@link AddressTerminated}。
         *
         * @param msg 地址终止事件
         */
        public void publish(AddressTerminated msg) {
            for (ActorRef sub : subscribers.get()) {
                sub.tell(msg, ActorRef.noSender);
            }
        }
    }
}
