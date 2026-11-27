/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.actor.ActorIdentity;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.ActorSelectionMessage;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.EmptyLocalActorRef;
import io.github.espen7.tapio.actor.Identify;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.LocalScope;
import io.github.espen7.tapio.actor.Nobody;
import io.github.espen7.tapio.actor.SystemGuardian;
import io.github.espen7.tapio.actor.VirtualPathContainer;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.dispatch.sysmsg.Watch;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * INTERNAL API：位于 {@code /remote} 路径的系统守护虚拟容器，处理远程部署创建与系统消息路由。
 */
@InternalApi
public final class RemoteDaemon extends VirtualPathContainer {
    private final ActorSystemImpl system;
    private final InternalActorRef terminator;
    private final AtomicBoolean terminating = new AtomicBoolean(false);

    /**
     * 构造 {@link RemoteDaemon}。
     *
     * @param system 所属 {@link ActorSystemImpl}
     * @param path {@code /remote} 路径
     * @param parent root guardian
     * @param terminator 终止协调 Actor
     * @param log 日志适配器
     */
    public RemoteDaemon(
            ActorSystemImpl system,
            ActorPath path,
            InternalActorRef parent,
            InternalActorRef terminator,
            LoggingAdapter log) {
        super(system.provider(), path, parent, log);
        this.system = system;
        this.terminator = terminator;
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        if (message instanceof Watch w) {
            w.watchee.sendSystemMessage(w);
        }
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        if (message instanceof DaemonMsgCreate dmc) {
            handleDaemonMsgCreate(dmc);
        } else if (message instanceof ActorSelectionMessage sel) {
            ActorSelection.deliverSelection(this, sender, sel);
        } else if (message instanceof Identify id) {
            sender.tell(new ActorIdentity(id.messageId(), Optional.of(this)), this);
        } else if (message == SystemGuardian.TerminationHook.INSTANCE) {
            terminating.set(true);
            terminator.tell(SystemGuardian.TerminationHookDone.INSTANCE, this);
        } else {
            super.tell(message, sender);
        }
    }

    private void handleDaemonMsgCreate(DaemonMsgCreate msg) {
        if (terminating.get()) {
            return;
        }
        if (!(msg.supervisor() instanceof InternalActorRef supervisor)) {
            return;
        }
        ActorPath targetPath;
        try {
            targetPath = ActorPath.fromString(msg.path());
        } catch (java.net.MalformedURLException e) {
            throw new IllegalArgumentException("Invalid remote actor path [" + msg.path() + "]", e);
        }
        Deploy localDeploy = new Deploy(
                msg.deploy().path(),
                LocalScope.INSTANCE,
                msg.deploy().dispatcher(),
                msg.deploy().mailbox());
        InternalActorRef child = system.provider().actorOf(
                system,
                msg.props(),
                supervisor,
                targetPath,
                false,
                Optional.of(localDeploy),
                true,
                false);
        addChild(targetPath.name(), child);
        child.start();
    }

    @Override
    public InternalActorRef getChild(Iterator<String> names) {
        if (!names.hasNext()) {
            return this;
        }
        String first = names.next();
        InternalActorRef child = getChild(first);
        if (child != Nobody.INSTANCE) {
            return child.getChild(names);
        }
        return new EmptyLocalActorRef(provider(), path().child(first), system.eventStream());
    }
}
