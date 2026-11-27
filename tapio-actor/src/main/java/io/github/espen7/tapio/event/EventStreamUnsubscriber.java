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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ConfigKeys;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.japi.pf.ReceiveBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * INTERNAL API：观察 EventStream 的订阅者，并在其终止时自动退订。
 */
final class EventStreamUnsubscriber extends AbstractActor {
    private static final AtomicInteger UNSUBSCRIBERS_COUNT = new AtomicInteger();

    private final EventStream eventStream;
    private final boolean debug;

    EventStreamUnsubscriber(EventStream eventStream, boolean debug) {
        this.eventStream = eventStream;
        this.debug = debug;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(Register.class, message -> {
                    if (debug) {
                        eventStream.publish(new Debug(Logging.simpleName(getClass()), getClass(),
                                "watching " + message.actor() + " in order to unsubscribe from EventStream when it terminates"));
                    }
                    if (message.actor() instanceof InternalActorRef) getContext().watch(message.actor());
                })
                .match(UnregisterIfNoMoreSubscribedChannels.class, message -> {
                    if (!eventStream.hasSubscriptions(message.actor())) {
                        if (debug) {
                            eventStream.publish(new Debug(Logging.simpleName(getClass()), getClass(),
                                    "unwatching " + message.actor() + ", since has no subscriptions"));
                        }
                        if (message.actor() instanceof InternalActorRef) getContext().unwatch(message.actor());
                    }
                })
                .match(Terminated.class, message -> {
                    if (debug) {
                        eventStream.publish(new Debug(Logging.simpleName(getClass()), getClass(),
                                "unsubscribe " + message.actor() + " from " + eventStream
                                        + ", because it was terminated"));
                    }
                    eventStream.unsubscribe(message.actor());
                })
                .build();
    }

    static ActorRef start(io.github.espen7.tapio.actor.ActorSystem system, EventStream eventStream) {
        boolean debug = system.settings().config().getBoolean(ConfigKeys.ACTOR_DEBUG_EVENT_STREAM);
        ActorRef unsubscriber = ((ExtendedActorSystem) system).systemActorOf(
                Props.create(EventStreamUnsubscriber.class, eventStream, debug)
                        .withDispatcher(Dispatchers.InternalDispatcherId),
                "eventStreamUnsubscriber-" + UNSUBSCRIBERS_COUNT.incrementAndGet());
        if (debug) {
            eventStream.publish(new Debug(Logging.simpleName(EventStreamUnsubscriber.class),
                    EventStreamUnsubscriber.class, "registering unsubscriber with " + eventStream));
        }
        eventStream.initUnsubscriber(unsubscriber);
        return unsubscriber;
    }

    record Register(ActorRef actor) {
    }

    record UnregisterIfNoMoreSubscribedChannels(ActorRef actor) {
    }
}
