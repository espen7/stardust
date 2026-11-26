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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.dispatch.sysmsg.Unwatch;
import io.github.espen7.tapio.dispatch.sysmsg.Watch;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 优雅停止目标 Actor 并异步等待其终止完成，对应 Pekko {@code GracefulStopSupport}。
 */
public final class GracefulStopSupport {
    private GracefulStopSupport() {
    }

    static CompletionStage<Boolean> gracefulStop(ActorRef target, Duration timeout, Object stopMessage) {
        Objects.requireNonNull(target, "target must not be null");
        Objects.requireNonNull(stopMessage, "stopMessage must not be null");
        if (!(target instanceof InternalActorRef internalTarget)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("target must be an InternalActorRef, was: " + target));
        }
        PromiseActorRef ref = PromiseActorRef.apply(
                internalTarget.provider(),
                timeout,
                target,
                stopMessage.getClass().getName(),
                target.path().name(),
                ActorRef.noSender);
        internalTarget.sendSystemMessage(new Watch(internalTarget, ref));
        target.tell(stopMessage, ActorRef.noSender);
        return ref.result().handle((value, failure) -> {
            if (failure != null) {
                internalTarget.sendSystemMessage(new Unwatch(target, ref));
                throw failure instanceof RuntimeException runtime ? runtime : new RuntimeException(failure);
            }
            if (value instanceof Terminated terminated && terminated.actor().path().equals(target.path())) {
                return Boolean.TRUE;
            }
            internalTarget.sendSystemMessage(new Unwatch(target, ref));
            return Boolean.FALSE;
        });
    }
}
