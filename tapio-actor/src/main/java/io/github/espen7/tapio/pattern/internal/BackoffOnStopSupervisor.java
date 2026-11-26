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

import io.github.espen7.tapio.actor.OneForOneStrategy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.SupervisorStrategy;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.pattern.BackoffReset;
import io.github.espen7.tapio.pattern.BackoffSupervisor;
import io.github.espen7.tapio.pattern.HandlingWhileStopped;
import io.github.espen7.tapio.pattern.RetrySupport;

import java.time.Duration;
import java.util.function.Predicate;

/**
 * 在子 Actor 自行停止时按指数退避延迟重启的监督者，对应 Pekko {@code BackoffOnStopSupervisor}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public final class BackoffOnStopSupervisor extends AbstractBackoffSupervisor {
    private final SupervisorStrategy supervisorStrategy;
    private final Predicate<Object> finalStopMessage;

    public BackoffOnStopSupervisor(Props childProps, String childName, Duration minBackoff, Duration maxBackoff,
                                   BackoffReset reset, double randomFactor, SupervisorStrategy strategy,
                                   HandlingWhileStopped handlingWhileStopped, Predicate<Object> finalStopMessage) {
        super(childProps, childName, minBackoff, maxBackoff, reset, randomFactor, handlingWhileStopped);
        this.finalStopMessage = finalStopMessage;
        if (strategy instanceof OneForOneStrategy oneForOne) {
            var defaultDecider = super.supervisorStrategy().decider();
            this.supervisorStrategy = new OneForOneStrategy(
                    oneForOne.maxNrOfRetries(), oneForOne.withinTimeRange(),
                    ex -> {
                        SupervisorStrategy.Directive fallback = defaultDecider.apply(ex);
                        SupervisorStrategy.Directive directive = oneForOne.decider().apply(ex);
                        return directive != null ? directive : fallback;
                    },
                    oneForOne.loggingEnabled());
        } else {
            this.supervisorStrategy = strategy;
        }
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return supervisorStrategy;
    }

    private Receive onTerminated() {
        return receiveBuilder()
                .match(Terminated.class, t -> t.actor().equals(child), t -> {
                    child = null;
                    if (finalStopMessageReceived) {
                        getContext().stop(getSelf());
                        return;
                    }
                    int maxNrOfRetries = supervisorStrategy instanceof OneForOneStrategy oneForOne
                            ? oneForOne.maxNrOfRetries()
                            : -1;
                    int nextRestartCount = restartCount + 1;
                    if (maxNrOfRetries == -1 || nextRestartCount <= maxNrOfRetries) {
                        Duration restartDelay = RetrySupport.calculateDelay(
                                restartCount, minBackoff, maxBackoff, randomFactor);
                        getContext().system().scheduler().scheduleOnce(
                                restartDelay, getSelf(), BackoffSupervisor.StartChild.INSTANCE,
                                getContext().dispatcher(), getSelf());
                        restartCount = nextRestartCount;
                    } else {
                        log().debug("Terminating on restart #{} which exceeds max allowed restarts ({})",
                                nextRestartCount, maxNrOfRetries);
                        getContext().stop(getSelf());
                    }
                })
                .build();
    }

    @Override
    public Receive createReceive() {
        return onTerminated().orElse(handleBackoff());
    }

    @Override
    protected void handleMessageToChild(Object msg) {
        if (child != null) {
            child.forward(msg, getContext());
            if (!finalStopMessageReceived && finalStopMessage != null) {
                finalStopMessageReceived = finalStopMessage.test(msg);
            }
        } else {
            if (finalStopMessage != null && finalStopMessage.test(msg)) {
                getContext().stop(getSelf());
            } else {
                forwardWhileStopped(msg);
            }
        }
    }
}
