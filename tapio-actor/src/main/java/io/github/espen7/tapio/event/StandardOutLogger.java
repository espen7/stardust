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
package io.github.espen7.tapio.event;

import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.InvalidMessageException;
import io.github.espen7.tapio.actor.MinimalActorRef;
import io.github.espen7.tapio.actor.RootActorPath;

/** 不依赖 ActorSystem、同步输出标准日志的 ActorRef。 */
public class StandardOutLogger extends MinimalActorRef implements StdOutLogger {
    /** 所有 ActorSystem 共享的标准输出 logger。 */
    public static final StandardOutLogger INSTANCE = new StandardOutLogger();

    private final ActorPath path = new RootActorPath(new Address(ActorPath.Scheme, "all-systems")).child("StandardOutLogger");

    /** 创建同步标准输出 logger。 */
    public StandardOutLogger() {
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public ActorRefProvider provider() {
        throw new UnsupportedOperationException("StandardOutLogger does not provide");
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        if (message == null) throw new InvalidMessageException("Message is null");
        print(message);
    }

    @Override
    public String toString() {
        return "StandardOutLogger";
    }
}
