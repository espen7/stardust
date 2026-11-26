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

import io.github.espen7.tapio.actor.dungeon.ChildrenContainer;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.sysmsg.Recreate;
import io.github.espen7.tapio.dispatch.sysmsg.Resume;
import io.github.espen7.tapio.dispatch.sysmsg.Suspend;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Terminate;
import io.github.espen7.tapio.event.Warning;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/** 在 supervisor 激活 ActorCell 前保存普通消息和系统消息。 */
final class UnstartedCell implements Cell {
    private final ActorSystemImpl systemImpl;
    private final RepointableActorRef self;
    private final Props props;
    private final InternalActorRef supervisor;
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedList<Envelope> queue = new LinkedList<>();
    private SystemMessage sysmsgQueue;

    UnstartedCell(ActorSystemImpl systemImpl, RepointableActorRef self, Props props, InternalActorRef supervisor) {
        this.systemImpl = systemImpl;
        this.self = self;
        this.props = props;
        this.supervisor = supervisor;
    }

    @Override
    public RepointableActorRef self() {
        return self;
    }

    @Override
    public ActorSystem system() {
        return systemImpl;
    }

    @Override
    public ActorSystemImpl systemImpl() {
        return systemImpl;
    }

    @Override
    public UnstartedCell start() {
        return this;
    }

    @Override
    public void suspend() {
        sendSystemMessage(new Suspend());
    }

    @Override
    public void resume(Throwable causedByFailure) {
        sendSystemMessage(new Resume(causedByFailure));
    }

    @Override
    public void restart(Throwable cause) {
        sendSystemMessage(new Recreate(cause));
    }

    @Override
    public void stop() {
        sendSystemMessage(new Terminate());
    }

    @Override
    public InternalActorRef parent() {
        return supervisor;
    }

    @Override
    public ChildrenContainer childrenRefs() {
        return ChildrenContainer.empty();
    }

    @Override
    public Optional<ChildRestartStats> getChildByName(String name) {
        return Optional.empty();
    }

    @Override
    public InternalActorRef getSingleChild(String name) {
        return Nobody.INSTANCE;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public Props props() {
        return props;
    }

    void replaceWith(Cell cell) {
        lock.lock();
        try {
            try {
                drainSysmsgQueue(cell);
                while (!queue.isEmpty()) {
                    cell.sendMessage(queue.poll());
                    drainSysmsgQueue(cell);
                }
            } finally {
                self.swapCell(cell);
            }
        } finally {
            lock.unlock();
        }
    }

    private void drainSysmsgQueue(Cell cell) {
        while (sysmsgQueue != null) {
            SystemMessage current = sysmsgQueue;
            sysmsgQueue = null;
            SystemMessage ordered = null;
            while (current != null) {
                SystemMessage next = current.next();
                current.next(ordered);
                ordered = current;
                current = next;
            }
            while (ordered != null) {
                SystemMessage next = ordered.next();
                ordered.unlink();
                cell.sendSystemMessage(ordered);
                ordered = next;
            }
        }
    }

    @Override
    public void sendMessage(Envelope message) {
        boolean acquired;
        try {
            acquired = lock.tryLock(systemImpl.settings().UnstartedPushTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            ActorCell.<RuntimeException>throwUnchecked(interrupted);
            return;
        }
        if (acquired) {
            try {
                Cell current = self.underlying();
                if (cellIsReady(current)) {
                    current.sendMessage(message);
                } else if (!queue.offer(message)) {
                    system().eventStream().publish(new Warning(self.path().toString(), getClass(),
                            "dropping message of type " + message.message().getClass() + " due to enqueue failure"));
                    system().deadLetters().tell(new DeadLetter(message.message(), message.sender(), self), message.sender());
                }
            } finally {
                lock.unlock();
            }
        } else {
            system().eventStream().publish(new Warning(self.path().toString(), getClass(),
                    "dropping message of type" + message.message().getClass() + " due to lock timeout"));
            system().deadLetters().tell(new DeadLetter(message.message(), message.sender(), self), message.sender());
        }
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        lock.lock();
        try {
            Cell current = self.underlying();
            if (cellIsReady(current)) {
                current.sendSystemMessage(message);
            } else {
                message.next(sysmsgQueue);
                sysmsgQueue = message;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isTerminated() {
        lock.lock();
        try {
            Cell current = self.underlying();
            return cellIsReady(current) && current.isTerminated();
        } finally {
            lock.unlock();
        }
    }

    private boolean cellIsReady(Cell cell) {
        return cell != this && cell != null;
    }

    @Override
    public boolean hasMessages() {
        lock.lock();
        try {
            Cell cell = self.underlying();
            return cellIsReady(cell) ? cell.hasMessages() : !queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int numberOfMessages() {
        lock.lock();
        try {
            Cell cell = self.underlying();
            return cellIsReady(cell) ? cell.numberOfMessages() : queue.size();
        } finally {
            lock.unlock();
        }
    }
}
