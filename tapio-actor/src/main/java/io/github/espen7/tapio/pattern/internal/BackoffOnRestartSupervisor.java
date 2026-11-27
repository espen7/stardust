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

import io.github.espen7.tapio.actor.ActorRef;
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

/**
 * 在子 Actor 发生需要 {@code Restart} 的失败时将其停止并按指数退避延迟重建的监督者。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public final class BackoffOnRestartSupervisor extends AbstractBackoffSupervisor {
    private final OneForOneStrategy supervisorStrategy;

    public BackoffOnRestartSupervisor(Props childProps, String childName, Duration minBackoff, Duration maxBackoff,
                                      BackoffReset reset, double randomFactor, OneForOneStrategy strategy,
                                      HandlingWhileStopped handlingWhileStopped) {
        super(childProps, childName, minBackoff, maxBackoff, reset, randomFactor, handlingWhileStopped);
        var defaultDecider = super.supervisorStrategy().decider();
        this.supervisorStrategy = new OneForOneStrategy(
                strategy.maxNrOfRetries(), strategy.withinTimeRange(),
                ex -> {
                    SupervisorStrategy.Directive fallback = defaultDecider.apply(ex);
                    SupervisorStrategy.Directive directive = strategy.decider().apply(ex);
                    if (directive == null) {
                        directive = fallback;
                    }
                    if (directive == SupervisorStrategy.Directive.Restart) {
                        int nextRestartCount = restartCount + 1;
                        Duration within = strategy.withinTimeRange();
                        if (within != null && !within.isNegative() && restartCount == 0) {
                            getContext().system().scheduler().scheduleOnce(
                                    within,
                                    getSelf(),
                                    new BackoffSupervisor.ResetRestartCount(nextRestartCount),
                                    getContext().dispatcher(),
                                    getSelf());
                        }
                        ActorRef childRef = getSender();
                        if (strategy.maxNrOfRetries() >= 0 && nextRestartCount > strategy.maxNrOfRetries()) {
                            log().debug("Terminating on restart #{} which exceeds max allowed restarts ({})",
                                    nextRestartCount, strategy.maxNrOfRetries());
                            getContext().become(createReceive());
                            getContext().stop(getSelf());
                        } else {
                            getContext().become(waitChildTerminatedBeforeBackoff(childRef).orElse(handleBackoff()));
                        }
                        return SupervisorStrategy.Directive.Stop;
                    }
                    return directive;
                },
                strategy.loggingEnabled());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return supervisorStrategy;
    }

    private Receive waitChildTerminatedBeforeBackoff(ActorRef childRef) {
        return receiveBuilder()
                .match(Terminated.class, t -> t.actor().equals(childRef), t -> {
                    getContext().become(createReceive());
                    child = null;
                    Duration restartDelay = RetrySupport.calculateDelay(restartCount, minBackoff, maxBackoff, randomFactor);
                    getContext().system().scheduler().scheduleOnce(
                            restartDelay, getSelf(), BackoffSupervisor.StartChild.INSTANCE,
                            getContext().dispatcher(), getSelf());
                    restartCount += 1;
                })
                .match(BackoffSupervisor.StartChild.class, ignored -> {
                })
                .build();
    }

    private Receive onTerminated() {
        return receiveBuilder()
                .match(Terminated.class, t -> {
                    log().debug("Terminating, because child [{}] terminated itself", t.actor());
                    getContext().stop(getSelf());
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
        } else {
            forwardWhileStopped(msg);
        }
    }
}
