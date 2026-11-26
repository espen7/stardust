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
package io.github.espen7.tapio.actor.dungeon;

import io.github.espen7.tapio.actor.Actor;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorInitializationException;
import io.github.espen7.tapio.actor.Cell;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.DispatchFailureReporting;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.Mailbox;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.ProducesMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMailbox;
import io.github.espen7.tapio.dispatch.sysmsg.Create;
import io.github.espen7.tapio.dispatch.sysmsg.Recreate;
import io.github.espen7.tapio.dispatch.sysmsg.Resume;
import io.github.espen7.tapio.dispatch.sysmsg.Supervise;
import io.github.espen7.tapio.dispatch.sysmsg.Suspend;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Terminate;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.util.NoStackTrace;
import io.github.espen7.tapio.util.NonFatal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Optional;

/** 创建、绑定和调度 ActorCell 的 Mailbox。 */
@InternalApi
public abstract class Dispatch implements Cell {
    private static final VarHandle mailboxHandle;

    static {
        try {
            mailboxHandle = MethodHandles.lookup().findVarHandle(Dispatch.class, "_mailboxDoNotCallMeDirectly", Mailbox.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    @SuppressWarnings("unused")
    private volatile Mailbox _mailboxDoNotCallMeDirectly;

    public final Mailbox mailbox() {
        return (Mailbox) mailboxHandle.getVolatile(this);
    }

    public final Mailbox swapMailbox(Mailbox newMailbox) {
        for (;;) {
            Mailbox oldMailbox = mailbox();
            if (mailboxHandle.compareAndSet(this, oldMailbox, newMailbox)) return oldMailbox;
        }
    }

    @Override
    public final boolean hasMessages() {
        return mailbox().hasMessages();
    }

    @Override
    public final int numberOfMessages() {
        return mailbox().numberOfMessages();
    }

    @Override
    public final boolean isTerminated() {
        return mailbox().isClosed();
    }

    public abstract MessageDispatcher dispatcher();

    /** 绑定 Mailbox 并排入 Create，然后按配置向 parent 报告监督关系。 */
    public final ActorCell init(boolean sendSupervise, MailboxType mailboxType) {
        ActorCell cell = (ActorCell) this;
        Mailbox mbox = dispatcher().createMailbox(cell, mailboxType);
        Class<? extends Actor> actorClass = props().actorClass();
        Create createMessage = new Create(Optional.empty());
        if (mailboxType instanceof ProducesMessageQueue<?> && systemImpl().mailboxes().hasRequiredType(actorClass)) {
            Class<?> req = systemImpl().mailboxes().getRequiredType(actorClass);
            if (!req.isInstance(mbox.messageQueue())) {
                String gotType = mbox.messageQueue() == null ? "null" : mbox.messageQueue().getClass().getName();
                createMessage = new Create(Optional.of(ActorInitializationException.apply(
                        self(), "Actor [" + self() + "] requires mailbox type [" + req + "] got [" + gotType + "]", null)));
            }
        }
        swapMailbox(mbox);
        mailbox().setActor(cell);
        mailbox().systemEnqueue(self(), createMessage);
        if (sendSupervise) parent().sendSystemMessage(new Supervise(self(), false));
        return cell;
    }

    public final ActorCell initWithFailure(Throwable failure) {
        ActorCell cell = (ActorCell) this;
        Mailbox mbox = dispatcher().createMailbox(cell, new UnboundedMailbox());
        swapMailbox(mbox);
        mailbox().setActor(cell);
        Create createMessage = new Create(Optional.of(ActorInitializationException.apply(
                self(), "failure while creating ActorCell", failure)));
        mailbox().systemEnqueue(self(), createMessage);
        return cell;
    }

    @Override
    public ActorCell start() {
        ActorCell cell = (ActorCell) this;
        dispatcher().attach(cell);
        return cell;
    }

    private void handleException(Throwable error) {
        ActorCell cell = (ActorCell) this;
        Class<?> sourceClass = cell.actor() == null ? cell.getClass() : cell.actor().getClass();
        if (error instanceof InterruptedException) {
            DispatchFailureReporting.report(system().eventStream(),
                    new Error(self().path().toString(), sourceClass, "interrupted during message send", error));
            Thread.currentThread().interrupt();
        } else if (NonFatal.apply(error)) {
            String message = error instanceof NoStackTrace
                    ? "swallowing exception during message send: " + error.getMessage()
                    : "swallowing exception during message send";
            DispatchFailureReporting.report(system().eventStream(),
                    new Error(self().path().toString(), sourceClass, message, error));
        } else {
            ActorCell.<RuntimeException>throwUnchecked(error);
        }
    }

    @Override
    public final void suspend() {
        sendSystemMessage(new Suspend());
    }

    @Override
    public final void resume(Throwable causedByFailure) {
        sendSystemMessage(new Resume(causedByFailure));
    }

    @Override
    public final void restart(Throwable cause) {
        sendSystemMessage(new Recreate(cause));
    }

    @Override
    public final void stop() {
        sendSystemMessage(new Terminate());
    }

    @Override
    public void sendMessage(Envelope message) {
        try {
            dispatcher().dispatch((ActorCell) this, message);
        } catch (Throwable error) {
            handleException(error);
        }
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        try {
            dispatcher().systemDispatch((ActorCell) this, message);
        } catch (Throwable error) {
            handleException(error);
        }
    }
}
