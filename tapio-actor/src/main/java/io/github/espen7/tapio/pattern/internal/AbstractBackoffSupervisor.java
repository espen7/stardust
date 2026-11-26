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
package io.github.espen7.tapio.pattern.internal;

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.pattern.BackoffReset;
import io.github.espen7.tapio.pattern.BackoffSupervisor;
import io.github.espen7.tapio.pattern.HandlingWhileStopped;

import java.time.Duration;
import java.util.Optional;

/**
 * 为 {@link BackoffOnRestartSupervisor} 与 {@link BackoffOnStopSupervisor} 提供公共退避逻辑，
 * 对应 Pekko {@code HandleBackoff}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public abstract class AbstractBackoffSupervisor extends AbstractLoggingActor {
    protected final Props childProps;
    protected final String childName;
    protected final Duration minBackoff;
    protected final Duration maxBackoff;
    protected final BackoffReset reset;
    protected final double randomFactor;
    protected final HandlingWhileStopped handlingWhileStopped;

    protected ActorRef child;
    protected int restartCount = 0;
    protected boolean finalStopMessageReceived = false;

    protected AbstractBackoffSupervisor(Props childProps, String childName, Duration minBackoff, Duration maxBackoff,
                                        BackoffReset reset, double randomFactor,
                                        HandlingWhileStopped handlingWhileStopped) {
        this.childProps = childProps;
        this.childName = childName;
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.reset = reset;
        this.randomFactor = randomFactor;
        this.handlingWhileStopped = handlingWhileStopped;
    }

    @Override
    public void preStart() {
        startChild();
    }

    protected final void startChild() {
        if (child == null) {
            child = getContext().watch(getContext().actorOf(childProps, childName));
        }
    }

    protected final Receive handleBackoff() {
        return receiveBuilder()
                .match(BackoffSupervisor.StartChild.class, msg -> {
                    startChild();
                    if (reset instanceof BackoffReset.AutoReset(Duration resetBackoff) && restartCount > 0) {
                        getContext().system().scheduler().scheduleOnce(
                                resetBackoff,
                                getSelf(),
                                new BackoffSupervisor.ResetRestartCount(restartCount),
                                getContext().dispatcher(),
                                getSelf());
                    }
                })
                .match(Object.class, msg -> msg == BackoffSupervisor.Reset, msg -> {
                    if (reset instanceof BackoffReset.ManualReset) {
                        restartCount = 0;
                    } else {
                        unhandled(msg);
                    }
                })
                .match(BackoffSupervisor.ResetRestartCount.class, msg -> {
                    if (msg.current() == restartCount && restartCount > 0) {
                        restartCount = 0;
                    }
                })
                .match(Object.class, msg -> msg == BackoffSupervisor.GetRestartCount,
                        msg -> getSender().tell(new BackoffSupervisor.RestartCount(restartCount), getSelf()))
                .match(Object.class, msg -> msg == BackoffSupervisor.GetCurrentChild,
                        msg -> getSender().tell(new BackoffSupervisor.CurrentChild(Optional.ofNullable(child)), getSelf()))
                .match(Object.class, msg -> child != null && child.equals(getSender()),
                        msg -> getContext().getParent().tell(msg, getSelf()))
                .matchAny(this::handleMessageToChild)
                .build();
    }

    protected final void forwardWhileStopped(Object msg) {
        switch (handlingWhileStopped) {
            case HandlingWhileStopped.ForwardDeathLetters ignored ->
                    getContext().system().deadLetters().forward(msg, getContext());
            case HandlingWhileStopped.ForwardTo(ActorRef handler) ->
                    handler.forward(msg, getContext());
            case HandlingWhileStopped.ReplyWith(Object replyMessage) ->
                    getSender().tell(replyMessage, getSelf());
        }
    }

    protected abstract void handleMessageToChild(Object msg);
}
