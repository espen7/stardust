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

import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.sysmsg.Supervise;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.util.NonFatal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.List;

/** 外部挂载时先缓冲消息，由 supervisor 将引用切换到真正的 ActorCell。 */
class RepointableActorRef extends ActorRefWithCell implements RepointableRef {
    private static final VarHandle cellHandle;
    private static final VarHandle lookupHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cellHandle = lookup.findVarHandle(RepointableActorRef.class, "_cellDoNotCallMeDirectly", Cell.class);
            lookupHandle = lookup.findVarHandle(RepointableActorRef.class, "_lookupDoNotCallMeDirectly", Cell.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private final ActorSystemImpl system;
    private final Props props;
    private final MessageDispatcher dispatcher;
    private final MailboxType mailboxType;
    private final InternalActorRef supervisor;
    private final ActorPath path;
    @SuppressWarnings("unused")
    private volatile Cell _cellDoNotCallMeDirectly;
    @SuppressWarnings("unused")
    private volatile Cell _lookupDoNotCallMeDirectly;

    RepointableActorRef(ActorSystemImpl system, Props props, MessageDispatcher dispatcher, MailboxType mailboxType,
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
        swapCell(new UnstartedCell(system, this, props, supervisor));
        swapLookup(underlying());
        supervisor.sendSystemMessage(new Supervise(this, async));
        if (!async) point(false);
        return this;
    }

    RepointableActorRef point(boolean catchFailures) {
        Cell current = underlying();
        if (current == null) throw new IllegalStateException("underlying cell is null");
        if (!(current instanceof UnstartedCell unstarted)) return this;
        Cell started;
        try {
            started = newCell(unstarted);
        } catch (Throwable failure) {
            if (!catchFailures || !NonFatal.apply(failure)) throw failure;
            MessageDispatcher safeDispatcher = system.dispatchers().defaultGlobalDispatcher();
            started = new ActorCell(system, this, props, safeDispatcher, supervisor).initWithFailure(failure);
        }
        swapLookup(started);
        started.start();
        unstarted.replaceWith(started);
        return this;
    }

    Cell newCell(UnstartedCell old) {
        return new ActorCell(system, this, props, dispatcher, supervisor).init(false, mailboxType);
    }

    @Override
    public Cell underlying() {
        return (Cell) cellHandle.getVolatile(this);
    }

    Cell lookup() {
        return (Cell) lookupHandle.getVolatile(this);
    }

    final Cell swapCell(Cell next) {
        for (;;) {
            Cell old = underlying();
            if (cellHandle.compareAndSet(this, old, next)) return old;
        }
    }

    final Cell swapLookup(Cell next) {
        for (;;) {
            Cell old = lookup();
            if (lookupHandle.compareAndSet(this, old, next)) return old;
        }
    }

    @Override
    public boolean isStarted() {
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
        return underlying().parent();
    }

    @Override
    InternalActorRef getSingleChild(String name) {
        return lookup().getSingleChild(name);
    }

    @Override
    List<ActorRef> children() {
        return lookup().childrenRefs().children();
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
        underlying().sendMessage(message, sender);
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        underlying().sendSystemMessage(message);
    }

    @Override
    public void start() {
    }

    @Override
    public void suspend() {
        underlying().suspend();
    }

    @Override
    public void resume(Throwable causedByFailure) {
        underlying().resume(causedByFailure);
    }

    @Override
    public void restart(Throwable cause) {
        underlying().restart(cause);
    }

    @Override
    public void stop() {
        underlying().stop();
    }

    @Override
    public boolean isLocal() {
        return underlying().isLocal();
    }

    @Override
    public boolean isTerminated() {
        return underlying().isTerminated();
    }
}
