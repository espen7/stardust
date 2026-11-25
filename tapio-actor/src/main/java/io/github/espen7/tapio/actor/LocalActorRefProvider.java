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

import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.dispatch.Dispatcher;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地 ActorRefProvider。
 *
 * <p>Provider 负责生成 ActorPath、分配 incarnation UID，并把新 Actor 挂到 parent
 * 的 children 集合。当前实现只支持同一 JVM 的 LocalActorRef，未来 Remote provider
 * 可以复用 ActorRefProvider 的抽象边界。</p>
 */
public class LocalActorRefProvider extends ActorRefProvider {
    private final ActorSystemImpl system;
    private final Deployer deployer;
    /** UID 区分同一路径上的不同 Actor incarnation。 */
    private final AtomicInteger uidGenerator = new AtomicInteger(1);
    private final LocalActorRef rootGuardian;
    private final LocalActorRef systemGuardian;
    private final DeadLetterActorRef deadLetters;
    private final AtomicLong tempNumber = new AtomicLong();
    private final ActorPath tempNode;
    private final VirtualPathContainer tempContainer;

    /**
     * 创建本地 Provider，并初始化临时引用容器、dead letters 和 user guardian。
     *
     * @param system 此 Provider 所属的 ActorSystem
     */
    public LocalActorRefProvider(ActorSystemImpl system) {
        this.system = system;
        this.deployer = new Deployer(system.settings());
        this.tempNode = new ActorPath(ActorPath.Scheme + "://" + system.name() + "/temp");
        this.tempContainer = new VirtualPathContainer(this, tempNode, system.log());
        this.deadLetters = new DeadLetterActorRef(this,
                new ActorPath(ActorPath.Scheme + "://" + system.name() + "/deadLetters"), system.eventStream());
        this.rootGuardian = new LocalActorRef(system, Props.create(Guardian.class), system.dispatcher(),
                system.mailboxes().lookup(Mailboxes.DefaultMailboxId), null,
                new ActorPath(ActorPath.Scheme + "://" + system.name() + "/user", uidGenerator.getAndIncrement()), "user");
        this.systemGuardian = new LocalActorRef(system, Props.create(SystemGuardian.class, rootGuardian),
                system.dispatcher(), system.mailboxes().lookup(Mailboxes.DefaultMailboxId),
                null,
                new ActorPath(ActorPath.Scheme + "://" + system.name() + "/system", uidGenerator.getAndIncrement()), "system");
    }

    /**
     * 创建 LocalActorRef，并异步向其 Mailbox 投递 Create。
     *
     * <p>按上游顺序先应用部署配置覆盖 Props，再校验并查找 Dispatcher，
     * 最后按选定 Dispatcher 的配置选择 mailbox type。所有配置错误都在创建
     * 引用前以 {@link ConfigurationException} 抛出。</p>
     */
    @Override
    public ActorRef actorOf(Props props, String name, ActorRef parent) {
        if (name == null || name.isBlank() || name.contains("/"))
            throw new IllegalArgumentException("Invalid actor name: " + name);
        ActorRef supervisor = parent == null ? rootGuardian : parent;
        String base = supervisor.path().toString();
        ActorPath path = new ActorPath(base + "/" + name, uidGenerator.getAndIncrement());

        Props props2 = resolveProps(props, path, supervisor);

        if (!system.dispatchers().hasDispatcher(props2.dispatcher()))
            throw new ConfigurationException(
                    "Dispatcher [" + props2.dispatcher() + "] not configured for path " + path);
        try {
            Dispatcher dispatcher = system.dispatchers().lookup(props2.dispatcher());
            MailboxType mailboxType = system.mailboxes().getMailboxType(props2,
                    system.dispatchers().config(props2.dispatcher()));
            return new LocalActorRef(system, props2, dispatcher, mailboxType, supervisor, path, name);
        } catch (RuntimeException error) {
            throw new ConfigurationException(
                    "configuration problem while creating [" + path + "] with dispatcher ["
                            + props2.dispatcher() + "] and mailbox [" + props2.mailbox() + "]", error);
        }
    }

    private Props resolveProps(Props props, ActorPath path, ActorRef supervisor) {
        String parentDispatcher = supervisor instanceof LocalActorRef local && local.underlying().props() != null
                ? local.underlying().props().dispatcher() : Deploy.NoDispatcherGiven;
        var deploy = deployer.lookup(path);
        if (deploy.isPresent()) {
            Deploy d = deploy.get();
            String dispatcher = d.dispatcher();
            String mailbox = d.mailbox();
            if (Deploy.NoDispatcherGiven.equals(dispatcher) && Deploy.NoMailboxGiven.equals(mailbox)) {
                return props;
            } else if (Deploy.DispatcherSameAsParent.equals(dispatcher) && Deploy.NoMailboxGiven.equals(mailbox)) {
                return props.withDispatcher(parentDispatcher);
            } else if (Deploy.NoMailboxGiven.equals(mailbox)) {
                return props.withDispatcher(dispatcher);
            } else if (Deploy.NoDispatcherGiven.equals(dispatcher)) {
                return props.withMailbox(mailbox);
            } else if (Deploy.DispatcherSameAsParent.equals(dispatcher)) {
                return props.withDispatcher(parentDispatcher).withMailbox(mailbox);
            }
            return props.withDispatcher(dispatcher).withMailbox(mailbox);
        }
        if (Deploy.DispatcherSameAsParent.equals(props.deploy().dispatcher())) {
            return props.withDispatcher(parentDispatcher);
        }
        return props;
    }

    @Override
    public ActorRef rootGuardian() {
        return rootGuardian;
    }

    @Override
    public ActorRef systemGuardian() {
        return systemGuardian;
    }

    @Override
    public ActorRef systemActorOf(Props props, String name) {
        return systemGuardian.underlying().attachChild(props, name, true);
    }

    @Override
    public ActorRef deadLetters() {
        return deadLetters;
    }

    @Override
    public Scheduler scheduler() {
        return system.scheduler();
    }

    @Override
    public ActorPath tempPath(String prefix) {
        String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+~";
        StringBuilder name = new StringBuilder(prefix).append('$');
        long number = tempNumber.getAndIncrement();
        do {
            name.append(alphabet.charAt((int) number & 63));
            number >>>= 6;
        } while (number != 0);
        return new ActorPath(tempNode, name.toString());
    }

    @Override
    public InternalActorRef tempContainer() {
        return tempContainer;
    }

    @Override
    public void registerTempActor(InternalActorRef actorRef, ActorPath path) {
        assert path.parent() == tempNode : "cannot registerTempActor() with anything not obtained from tempPath()";
        tempContainer.addChild(path.name(), actorRef);
    }

    @Override
    public void unregisterTempActor(ActorPath path) {
        assert path.parent() == tempNode : "cannot unregisterTempActor() with anything not obtained from tempPath()";
        tempContainer.removeChild(path.name());
    }

    private static class Guardian extends AbstractActor {
        @Override
        public void preRestart(Throwable reason, Object message) {
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Terminated.class, ignored -> getContext().stop(getSelf()))
                    .match(SystemMessage.StopChild.class, stop -> getContext().stop(stop.child))
                    .build();
        }
    }

    private static final class SystemGuardian extends Guardian {
        private final ActorRef guardian;
        private final Set<ActorRef> terminationHooks = new HashSet<>();
        private boolean terminating;

        private SystemGuardian(ActorRef guardian) {
            this.guardian = guardian;
        }

        @Override
        public void preStart() {
            getContext().watch(guardian);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Terminated.class, terminated -> {
                        if (terminated.actor().equals(guardian)) {
                            terminating = true;
                            for (ActorRef hook : Set.copyOf(terminationHooks)) {
                                hook.tell(new SystemMessage.TerminationHook(), getSelf());
                            }
                            stopWhenAllTerminationHooksDone();
                        } else {
                            terminationHooks.remove(terminated.actor());
                            stopWhenAllTerminationHooksDone();
                        }
                    })
                    .match(SystemMessage.StopChild.class, stop -> getContext().stop(stop.child))
                    .match(SystemMessage.RegisterTerminationHook.class, register -> {
                        if (!terminating && register.hook != null) {
                            terminationHooks.add(register.hook);
                            getContext().watch(register.hook);
                        }
                    })
                    .match(SystemMessage.TerminationHookDone.class, ignored -> {
                        terminationHooks.remove(getSender());
                        stopWhenAllTerminationHooksDone();
                    })
                    .build();
        }

        private void stopWhenAllTerminationHooksDone() {
            if (terminating && terminationHooks.isEmpty()) getContext().stop(getSelf());
        }
    }
}
