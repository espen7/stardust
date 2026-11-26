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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.util.Subclassification;
import io.github.espen7.tapio.util.SubclassifiedIndex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** 订阅变更适用于指定 channel 及其子 channel。 */
abstract class SubchannelClassification extends LoggingBus {
    private final SubclassifiedIndex<Class<?>, ActorRef> subscriptions = new SubclassifiedIndex<>(subclassification());
    private volatile Map<Class<?>, Set<ActorRef>> cache = Map.of();

    protected abstract Subclassification<Class<?>> subclassification();

    protected abstract Class<?> classify(Object event);

    protected abstract void publish(Object event, ActorRef subscriber);

    @Override
    public boolean subscribe(ActorRef subscriber, Class<?> to) {
        synchronized (subscriptions) {
            Map<Class<?>, Set<ActorRef>> diff = subscriptions.addValue(to, subscriber);
            addToCache(diff);
            return !diff.isEmpty();
        }
    }

    @Override
    public boolean unsubscribe(ActorRef subscriber, Class<?> from) {
        synchronized (subscriptions) {
            Map<Class<?>, Set<ActorRef>> diff = subscriptions.removeValue(from, subscriber);
            Map<Class<?>, Set<ActorRef>> updated = new HashMap<>(cache);
            updated.putAll(diff);
            cache = Map.copyOf(updated);
            return !diff.isEmpty();
        }
    }

    @Override
    public void unsubscribe(ActorRef subscriber) {
        synchronized (subscriptions) {
            removeFromCache(subscriptions.removeValue(subscriber));
        }
    }

    @Override
    public void publish(Object event) {
        Class<?> classifier = classify(event);
        Set<ActorRef> recipients = cache.get(classifier);
        if (recipients == null) {
            synchronized (subscriptions) {
                recipients = cache.get(classifier);
                if (recipients == null) {
                    addToCache(subscriptions.addKey(classifier));
                    recipients = cache.get(classifier);
                }
            }
        }
        for (ActorRef recipient : recipients) publish(event, recipient);
    }

    @InternalApi
    public boolean hasSubscriptions(ActorRef subscriber) {
        return cache.values().stream().anyMatch(subscribers -> subscribers.contains(subscriber));
    }

    private void removeFromCache(Map<Class<?>, Set<ActorRef>> changes) {
        Map<Class<?>, Set<ActorRef>> updated = new HashMap<>(cache);
        changes.forEach((classifier, subscribers) -> {
            Set<ActorRef> remaining = new HashSet<>(updated.getOrDefault(classifier, Set.of()));
            remaining.removeAll(subscribers);
            updated.put(classifier, Set.copyOf(remaining));
        });
        cache = Map.copyOf(updated);
    }

    private void addToCache(Map<Class<?>, Set<ActorRef>> changes) {
        Map<Class<?>, Set<ActorRef>> updated = new HashMap<>(cache);
        changes.forEach((classifier, subscribers) -> {
            Set<ActorRef> combined = new HashSet<>(updated.getOrDefault(classifier, Set.of()));
            combined.addAll(subscribers);
            updated.put(classifier, Set.copyOf(combined));
        });
        cache = Map.copyOf(updated);
    }
}
