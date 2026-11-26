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
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.InternalActorRef;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** 使用临时引用等待选择中的首个回复。 */
public final class AskableActorSelection {
    private final ActorSelection selection;

    AskableActorSelection(ActorSelection selection) {
        this.selection = selection;
    }

    CompletionStage<Object> internalAsk(Object message, Duration timeout, ActorRef sender) {
        if (timeout.isZero() || timeout.isNegative()) {
            return CompletableFuture.failedStage(AskableActorRef.negativeTimeoutException(selection, message, sender));
        }
        if (!(selection.anchor() instanceof InternalActorRef anchor)) {
            return CompletableFuture.failedStage(AskableActorRef.unsupportedRecipientType(selection, message, sender));
        }
        String prefix = URLEncoder.encode(selection.pathString().replace('/', '_'), StandardCharsets.UTF_8);
        return PromiseActorRef.apply(anchor.provider(), timeout, selection, message.getClass().getName(), prefix, sender)
                .ask(selection, message);
    }
}
