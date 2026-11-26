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

import io.github.espen7.tapio.actor.Actor;
import io.github.espen7.tapio.actor.ActorLogMarker;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.AllDeadLetters;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.DeadLetterActorRef;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.Dropped;
import io.github.espen7.tapio.actor.UnhandledMessage;
import io.github.espen7.tapio.actor.WrappedMessage;
import io.github.espen7.tapio.japi.pf.PartialFunction;
import io.github.espen7.tapio.japi.pf.ReceiveBuilder;

import java.time.Duration;
import java.util.Optional;

/** 订阅 dead-letter 事件，并按照系统设置限制日志数量。 */
public final class DeadLetterListener extends Actor {
    private final int maxCount;
    private final Duration suspendDuration;
    private final String suspendDescription;
    private int count;
    private long suspendDeadline;

    /**
     * 创建默认 dead-letter 日志订阅者。
     *
     * @param maxCount 一轮内最多记录的事件数
     * @param suspendDuration 达到上限后的暂停时间；为 {@code null} 时停止订阅者
     * @param suspendDescription 暂停时间的文本表示
     */
    public DeadLetterListener(int maxCount, Duration suspendDuration, String suspendDescription) {
        this.maxCount = maxCount;
        this.suspendDuration = suspendDuration;
        this.suspendDescription = suspendDescription;
    }

    @Override
    public void preStart() {
        EventStream stream = context.system().eventStream();
        stream.subscribe(self, DeadLetter.class);
        stream.subscribe(self, Dropped.class);
        stream.subscribe(self, UnhandledMessage.class);
    }

    @Override
    public void postStop() {
        context.system().eventStream().unsubscribe(self);
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
    }

    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public PartialFunction<Object, Void> receive() {
        return ReceiveBuilder.create().match(AllDeadLetters.class, this::receiveDeadLetter).build().onMessage();
    }

    private void receiveDeadLetter(AllDeadLetters event) {
        if (event.message() instanceof WrappedMessage wrapped && wrapped.message() instanceof DeadLetterSuppression) return;
        if (count == Integer.MAX_VALUE) {
            Logging.getLogger(context.system().eventStream(), self.path().toString(), getClass())
                    .info("Resetting DeadLetterListener counter after reaching Int.MaxValue.");
            count = 1;
        } else count++;
        if (maxCount == Integer.MAX_VALUE) {
            logDeadLetter(event, "");
        } else if (suspendDuration == null) {
            if (count == maxCount) {
                logDeadLetter(event, ", no more dead letters will be logged");
                context.stop(self);
            } else logDeadLetter(event, "");
        } else if (count <= maxCount) {
            if (count == maxCount) {
                logDeadLetter(event, ", no more dead letters will be logged in next [" + suspendDescription + "]");
                suspendDeadline = System.nanoTime() + suspendDuration.toNanos();
                context.become(receiveWhenSuspended());
            } else logDeadLetter(event, "");
        }
    }

    private PartialFunction<Object, Void> receiveWhenSuspended() {
        return ReceiveBuilder.create()
                .match(AllDeadLetters.class, event -> {
                    if (event.message() instanceof WrappedMessage wrapped
                            && wrapped.message() instanceof DeadLetterSuppression) return;
                    if (System.nanoTime() - suspendDeadline > 0) {
                        logDeadLetter(event, ", of which " + (count - maxCount) + " were not logged. The counter will be reset now");
                        count = 0;
                        context.become(receive());
                    } else {
                        count++;
                    }
                })
                .build().onMessage();
    }

    private void logDeadLetter(AllDeadLetters event, String doneMsg) {
        String origin = isReal(event.sender()) ? " from " + event.sender() : "";
        Object unwrapped = WrappedMessage.unwrap(event.message());
        String messageClass = unwrapped.getClass().getName();
        String wrappedIn = event.message() instanceof WrappedMessage ? " wrapped in [" + event.message().getClass().getName() + "]" : "";
        String message;
        if (event instanceof Dropped dropped) {
            String destination = isReal(event.recipient()) ? " to " + event.recipient() : "";
            message = "Message [" + messageClass + "]" + wrappedIn + origin + destination + " was dropped. " + dropped.reason() + ". "
                    + "[" + count + "] dead letters encountered" + doneMsg + ". ";
        } else if (event instanceof UnhandledMessage) {
            String destination = isReal(event.recipient()) ? " to " + event.recipient() : "";
            message = "Message [" + messageClass + "]" + wrappedIn + origin + destination + " was unhandled. "
                    + "[" + count + "] dead letters encountered" + doneMsg + ". ";
        } else {
            message = "Message [" + messageClass + "]" + wrappedIn + origin + " to " + event.recipient() + " was not delivered. "
                    + "[" + count + "] dead letters encountered" + doneMsg + ". "
                    + "If this is not an expected behavior then " + event.recipient() + " may have terminated unexpectedly. ";
        }
        context.system().eventStream().publish(new Info(event.recipient().path().toString(), event.recipient().getClass(),
                message + "This logging can be turned off or adjusted with configuration settings 'tapio.log-dead-letters' "
                        + "and 'tapio.log-dead-letters-during-shutdown'.",
                ActorLogMarker.deadLetter(messageClass)));
    }

    private boolean isReal(ActorRef actor) {
        return actor != ActorRef.noSender && actor != context.system().deadLetters() && !(actor instanceof DeadLetterActorRef);
    }
}
