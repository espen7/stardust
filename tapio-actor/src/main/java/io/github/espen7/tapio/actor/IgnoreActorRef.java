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

import io.github.espen7.tapio.annotation.InternalApi;

/** 丢弃收到的普通消息和系统消息的内部引用。 */
@InternalApi
final class IgnoreActorRef extends MinimalActorRef {
    private static final ActorPath path = new RootActorPath(new Address(ActorPath.Scheme, "local")).child("ignore");
    private final ActorRefProvider provider;

    static boolean isIgnoreRefPath(ActorPath p) {
        return path.equals(p);
    }

    IgnoreActorRef(ActorRefProvider provider) {
        this.provider = provider;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public ActorRefProvider provider() {
        return provider;
    }
}
