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

import io.github.espen7.tapio.dispatch.Dispatcher;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.List;

/** 外部挂载时先缓冲消息，由 supervisor 将引用切换到真正的 ActorCell。 */
final class RepointableActorRef extends ActorRefWithCell {
    private static final VarHandle cellHandle;
    private static final VarHandle lookupHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cellHandle = lookup.findVarHandle(RepointableActorRef.class, "cell", Cell.class);
            lookupHandle = lookup.findVarHandle(RepointableActorRef.class, "lookup", Cell.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private final ActorSystemImpl system;
    private final Props props;
    private final Dispatcher dispatcher;
    private final MailboxType mailboxType;
    private final InternalActorRef supervisor;
    private final ActorPath path;
    @SuppressWarnings("unused")
    private volatile Cell cell;
    @SuppressWarnings("unused")
    private volatile Cell lookup;

    RepointableActorRef(ActorSystemImpl system, Props props, Dispatcher dispatcher, MailboxType mailboxType,
                        InternalActorRef supervisor, ActorPath path) {
        this.system = system;
        this.props = props;
        this.dispatcher = dispatcher;
        this.mailboxType = mailboxType;
        this.supervisor = supervisor;
        this.path = path;
    }

    RepointableActorRef initialize(boolean async) {
        if (underlying() != null) throw new IllegalStateException("initialize called more than once!");
        Cell unstarted = new UnstartedCell(system, this);
        swapCell(unstarted);
        swapLookup(unstarted);
        supervisor.sendSystemMessage(new SystemMessage.Supervise(this, async));
        if (!async) point(false);
        return this;
    }

    void point(boolean catchFailures) {
        Cell current = underlying();
        if (current == null) throw new IllegalStateException("underlying cell is null");
        if (!(current instanceof UnstartedCell unstarted)) return;
        ActorCell started;
        try {
            started = newCell();
        } catch (RuntimeException failure) {
            if (!catchFailures) throw failure;
            started = new ActorCell(system, this, props, system.dispatchers().defaultGlobalDispatcher(), supervisor);
            started.initWithFailure(failure);
        }
        swapLookup(started);
        started.start();
        unstarted.replaceWith(started);
    }

    private ActorCell newCell() {
        ActorCell started = new ActorCell(system, this, props, dispatcher, supervisor);
        started.init(false, mailboxType);
        return started;
    }

    @Override
    public Cell underlying() {
        return (Cell) cellHandle.getVolatile(this);
    }

    private Cell lookup() {
        return (Cell) lookupHandle.getVolatile(this);
    }

    void swapCell(Cell next) {
        cellHandle.getAndSet(this, next);
    }

    private void swapLookup(Cell next) {
        lookupHandle.getAndSet(this, next);
    }

    boolean isStarted() {
        Cell current = underlying();
        if (current == null) throw new IllegalStateException("isStarted called before initialized");
        return !(current instanceof UnstartedCell);
    }

    @Override
    ActorSystemImpl system() {
        return system;
    }

    @Override
    Props props() {
        return props;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public ActorRefProvider provider() {
        return system.provider();
    }

    @Override
    InternalActorRef getParent() {
        return supervisor;
    }

    @Override
    InternalActorRef getSingleChild(String name) {
        return lookup() instanceof ActorCell actorCell ? actorCell.getSingleChild(name) : null;
    }

    @Override
    List<ActorRef> children() {
        return lookup() instanceof ActorCell actorCell ? actorCell.childrenRefs().children() : List.of();
    }

    @Override
    InternalActorRef getChild(Iterator<String> names) {
        while (names.hasNext()) {
            String name = names.next();
            if (name.isEmpty()) continue;
            InternalActorRef next = name.equals("..") ? getParent() : getSingleChild(name);
            return next == null ? null : next.getChild(names);
        }
        return this;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        underlying().sendMessage(Envelope.apply(message, sender, system));
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        underlying().sendSystemMessage(message);
    }

    @Override
    public void stop() {
        sendSystemMessage(new SystemMessage.Terminate());
    }

    @Override
    public boolean isTerminated() {
        return underlying().isTerminated();
    }
}
