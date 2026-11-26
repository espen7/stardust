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

/**
 * 内部单级或路径查找未命中时返回的空占位引用，对应 Pekko {@code Nobody}。
 */
@InternalApi
final class Nobody extends MinimalActorRef {
    static final Nobody INSTANCE = new Nobody();
    private static final ActorPath PATH =
            new ActorPath(new ActorPath(new Address(ActorPath.Scheme, "all-systems")), "Nobody");

    private Nobody() {
    }

    @Override
    public ActorPath path() {
        return PATH;
    }

    @Override
    public ActorRefProvider provider() {
        throw new UnsupportedOperationException("Nobody does not provide");
    }
}
