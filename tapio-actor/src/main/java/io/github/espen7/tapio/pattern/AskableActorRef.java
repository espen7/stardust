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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 按目标引用类型、终止状态和超时参数分派 Classic ask。
 */
final class AskableActorRef {
    private final ActorRef actorRef;

    AskableActorRef(ActorRef actorRef) {
        this.actorRef = actorRef;
    }

    CompletionStage<Object> internalAsk(Object message, Duration timeout, ActorRef sender) {
        if (actorRef instanceof InternalActorRef ref) {
            if (ref.isTerminated()) {
                actorRef.tell(message);
                return CompletableFuture.failedStage(recipientTerminatedException(actorRef, message, sender));
            }
            if (timeout.isZero() || timeout.isNegative()) {
                return CompletableFuture.failedStage(negativeTimeoutException(actorRef, message, sender));
            }
            return PromiseActorRef.apply(ref.provider(), timeout, actorRef, message.getClass().getName(),
                    ref.path().name(), sender).ask(actorRef, message);
        }
        return CompletableFuture.failedStage(unsupportedRecipientType(actorRef, message, sender));
    }

    private static String messagePartOfException(Object message, ActorRef sender) {
        Object msg = message == null ? "unknown" : message;
        String wasSentBy = sender == ActorRef.noSender ? "" : " was sent by [" + sender + "]";
        return "Message of type [" + msg.getClass().getName() + "]" + wasSentBy + ".";
    }

    static IllegalArgumentException negativeTimeoutException(Object recipient, Object message, ActorRef sender) {
        return new IllegalArgumentException("Timeout length must be positive, question not sent to [" + recipient + "]. "
                + messagePartOfException(message, sender));
    }

    private static AskTimeoutException recipientTerminatedException(ActorRef recipient, Object message, ActorRef sender) {
        return new AskTimeoutException("Recipient [" + recipient + "] had already been terminated. "
                + messagePartOfException(message, sender));
    }

    private static IllegalArgumentException unsupportedRecipientType(ActorRef recipient, Object message, ActorRef sender) {
        return new IllegalArgumentException("Unsupported recipient type, question not sent to [" + recipient + "]. "
                + messagePartOfException(message, sender));
    }
}
