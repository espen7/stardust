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
package io.github.espen7.tapio.actor;

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.DequeBasedMessageQueueSemantics;
import io.github.espen7.tapio.dispatch.Envelope;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * 为 {@link AbstractActorWithStash} 等基类提供内部暂存队列管理。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
final class StashSupport {
    private final ActorCell cell;
    private final ActorRef self;
    private final int capacity;
    private final DequeBasedMessageQueueSemantics mailbox;
    private final List<Envelope> theStash = new ArrayList<>();

    StashSupport(ActorContext context, ActorRef self) {
        this.cell = (ActorCell) context;
        this.self = self;
        this.capacity = context.system().mailboxes().stashCapacity(context.props().dispatcher(), context.props().mailbox());
        if (cell.mailbox().messageQueue() instanceof DequeBasedMessageQueueSemantics deque) {
            this.mailbox = deque;
        } else {
            throw ActorInitializationException.apply(self,
                    "DequeBasedMailbox required, got: " + cell.mailbox().messageQueue().getClass().getName() + "\n"
                            + "An (unbounded) deque-based mailbox can be configured as follows:\n"
                            + "  my-custom-mailbox {\n"
                            + "    mailbox-type = \"io.github.espen7.tapio.dispatch.UnboundedDequeBasedMailbox\"\n"
                            + "  }\n", null);
        }
    }

    void stash() {
        Envelope currMsg = cell.currentMessage();
        if (!theStash.isEmpty() && currMsg == theStash.getLast()) {
            throw new IllegalStateException("Can't stash the same message " + currMsg + " more than once");
        }
        if (capacity <= 0 || theStash.size() < capacity) {
            theStash.add(currMsg);
        } else {
            throw new StashOverflowException("Couldn't enqueue message "
                    + currMsg.message().getClass().getName() + " from " + currMsg.sender() + " to stash of " + self);
        }
    }

    void unstash() {
        if (!theStash.isEmpty()) {
            try {
                enqueueFirst(theStash.getFirst());
            } finally {
                theStash.removeFirst();
            }
        }
    }

    void unstashAll() {
        unstashAll(ignored -> true);
    }

    void unstashAll(Predicate<Object> filterPredicate) {
        try {
            for (int i = theStash.size() - 1; i >= 0; i--) {
                Envelope envelope = theStash.get(i);
                if (filterPredicate.test(envelope.message())) {
                    enqueueFirst(envelope);
                }
            }
        } finally {
            theStash.clear();
        }
    }

    private void enqueueFirst(Envelope envelope) {
        mailbox.enqueueFirst(self, envelope);
        if (envelope.message() instanceof Terminated terminated) {
            cell.terminatedQueuedFor(terminated.actor(), Optional.empty());
        }
    }
}
