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

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.Actor;
import io.github.espen7.tapio.actor.ActorLogMarker;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.AllDeadLetters;
import io.github.espen7.tapio.actor.ConfigKeys;
import io.github.espen7.tapio.actor.CoordinatedShutdown;
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
    private final EventStream eventStream;
    private final int maxCount;
    private final boolean isAlwaysLoggingDeadLetters;
    private final boolean logDeadLettersDuringShutdown;
    private final Config coordinatedShutdownConfig;
    private final Duration suspendDuration;
    private final String suspendDescription;
    private CoordinatedShutdown coordinatedShutdown;
    private Boolean shutdownTerminatesSystem;
    private int count;
    private long suspendDeadline;

    /**
     * 创建默认 dead-letter 日志订阅者。
     */
    public DeadLetterListener() {
        ActorSystem.Settings settings = context.system().settings();
        this.eventStream = context.system().eventStream();
        this.maxCount = settings.LogDeadLetters;
        this.isAlwaysLoggingDeadLetters = maxCount == Integer.MAX_VALUE;
        this.logDeadLettersDuringShutdown = settings.LogDeadLettersDuringShutdown;
        this.coordinatedShutdownConfig = settings.config().getConfig(ConfigKeys.COORDINATED_SHUTDOWN);
        this.suspendDuration = settings.LogDeadLettersSuspendDuration;
        this.suspendDescription = settings.LogDeadLettersSuspendDuration == null
                ? "infinite" : settings.LogDeadLettersSuspendDuration.toString();
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

    private void incrementCount() {
        if (count == Integer.MAX_VALUE) {
            Logging.getLogger(context.system(), self.path().toString(), getClass())
                    .info("Resetting DeadLetterListener counter after reaching Int.MaxValue.");
            count = 1;
        } else {
            count++;
        }
    }

    private void receiveDeadLetter(AllDeadLetters event) {
        if (isSuppressed(event)) return;
        incrementCount();
        if (isAlwaysLoggingDeadLetters) {
            logDeadLetter(event, "");
        } else if (suspendDuration == null) {
            if (count == maxCount) {
                logDeadLetter(event, ", no more dead letters will be logged");
                context.stop(self);
            } else logDeadLetter(event, "");
        } else {
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
                    if (isSuppressed(event)) return;
                    incrementCount();
                    if (System.nanoTime() - suspendDeadline > 0) {
                        logDeadLetter(event, ", of which " + (count - maxCount - 1)
                                + " were not logged. The counter will be reset now");
                        count = 0;
                        context.become(receive());
                    }
                })
                .build().onMessage();
    }

    private boolean isSuppressed(AllDeadLetters event) {
        return isWrappedSuppressed(event) || suppressedDuringShutdown();
    }

    private boolean isWrappedSuppressed(AllDeadLetters event) {
        return event.message() instanceof WrappedMessage wrapped
                && wrapped.message() instanceof DeadLetterSuppression;
    }

    private boolean suppressedDuringShutdown() {
        return !logDeadLettersDuringShutdown && terminatingShutdownInProgress();
    }

    private boolean terminatingShutdownInProgress() {
        if (coordinatedShutdown == null) {
            coordinatedShutdown = CoordinatedShutdown.get(context.system());
        }
        Optional<CoordinatedShutdown.Reason> reason = coordinatedShutdown.shutdownReason();
        if (reason.isEmpty()) return false;
        if (shutdownTerminatesSystem == null) {
            shutdownTerminatesSystem = CoordinatedShutdown
                    .confWithOverrides(coordinatedShutdownConfig, reason)
                    .getBoolean("terminate-actor-system");
        }
        return shutdownTerminatesSystem;
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
