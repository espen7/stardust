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
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.serialization.Serialization;
import io.github.espen7.tapio.util.Helpers;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地 ActorRefProvider。
 *
 * <p>Provider 管理本地 guardian、引用、地址和部署配置，并按调用方分配的 ActorPath 创建引用。</p>
 */
public class LocalActorRefProvider extends ActorRefProvider {
    private final ActorSystemImpl system;
    private final Deployer deployer;
    private final LoggingAdapter log;
    private final LoggingAdapter logDeser;
    private final long systemUid = ThreadLocalRandom.current().nextLong();
    private final ActorPath rootPath;
    private final InternalActorRef theOneWhoWalksTheBubblesOfSpaceTime;
    private LocalActorRef rootGuardian;
    private LocalActorRef guardian;
    private LocalActorRef systemGuardian;
    private final DeadLetterActorRef deadLetters;
    private final IgnoreActorRef ignoreRef;
    private final CompletableFuture<Terminated> terminationPromise = new CompletableFuture<>();
    private final AtomicLong tempNumber = new AtomicLong();
    private final ActorPath tempNode;
    private VirtualPathContainer tempContainer;
    private Serialization.Information serializationInformationCache;

    /**
     * 创建本地 Provider、根路径和内部引用。ActorSystem 发布 Provider 后再启动 guardian。
     *
     * @param system 此 Provider 所属的 ActorSystem
     */
    public LocalActorRefProvider(ActorSystemImpl system) {
        this.system = system;
        this.deployer = new Deployer(system.settings());
        this.rootPath = new ActorPath(new Address(ActorPath.Scheme, system.name()));
        this.log = Logging.getLogger(system.eventStream(), LocalActorRefProvider.class.getSimpleName(),
                LocalActorRefProvider.class);
        this.logDeser = Logging.getLogger(system.eventStream(), getClass().getName() + ".Deserialization",
                LocalActorRefProvider.class);
        this.deadLetters = new DeadLetterActorRef(this, new ActorPath(rootPath, "deadLetters"), system.eventStream());
        this.ignoreRef = new IgnoreActorRef(this);
        this.tempNode = new ActorPath(rootPath, "temp");
        this.theOneWhoWalksTheBubblesOfSpaceTime = new MinimalActorRef() {
            private final CompletableFuture<Terminated> causeOfTermination = new CompletableFuture<>();
            private final ActorPath path = new ActorPath(rootPath, "bubble-walker");

            @Override
            public ActorPath path() {
                return path;
            }

            @Override
            public ActorRefProvider provider() {
                return LocalActorRefProvider.this;
            }

            @Override
            public boolean isTerminated() {
                return causeOfTermination.isDone();
            }

            @Override
            public void stop() {
                causeOfTermination.complete(new Terminated(rootGuardian, true, true));
                causeOfTermination.whenComplete((terminated, failure) -> {
                    if (failure == null) terminationPromise.complete(terminated);
                    else terminationPromise.completeExceptionally(failure);
                });
            }

            @Override
            public void tell(Object message, ActorRef sender) {
                if (isTerminated()) return;
                if (message == null) throw new InvalidMessageException("Message is null");
                log.error("{} received unexpected message [{}]", this, message);
            }

            @Override
            public void sendSystemMessage(SystemMessage message) {
                if (isTerminated()) return;
                if (message instanceof SystemMessage.Failed failed) {
                    log.error(failed.cause, "guardian {} failed, shutting down!", failed.child);
                    causeOfTermination.completeExceptionally(failed.cause);
                    failed.child.stop();
                } else if (message instanceof SystemMessage.DeathWatchNotification) {
                    stop();
                } else if (!(message instanceof SystemMessage.Supervise)) {
                    log.error("{} received unexpected system message [{}]", this, message);
                }
            }
        };
    }

    @Override
    void init(ActorSystemImpl system) {
        this.rootGuardian = new LocalActorRef(system, Props.create(Guardian.class, rootGuardianStrategy()),
                system.dispatchers().internalDispatcher(), system.mailboxes().lookup(Mailboxes.DefaultMailboxId),
                theOneWhoWalksTheBubblesOfSpaceTime, rootPath, rootPath.name(), false) {
            @Override
            InternalActorRef getParent() {
                return this;
            }

            @Override
            InternalActorRef getSingleChild(String name) {
                return switch (name) {
                    case "temp" -> tempContainer;
                    case "deadLetters" -> deadLetters;
                    default -> super.getSingleChild(name);
                };
            }
        };
        this.tempContainer = new VirtualPathContainer(this, tempNode, rootGuardian, log);
        rootGuardian.start();
        ActorCell cell = rootGuardian.underlying();
        MailboxType defaultMailbox = system.mailboxes().lookup(Mailboxes.DefaultMailboxId);

        cell.reserveChild("user");
        guardian = new LocalActorRef(system, Props.create(Guardian.class, guardianSupervisorStrategy(system)),
                system.dispatchers().internalDispatcher(), defaultMailbox, rootGuardian,
                new ActorPath(rootPath, "user"), "user", false);
        cell.initChild(guardian);
        guardian.start();

        cell.reserveChild("system");
        systemGuardian = new LocalActorRef(system, Props.create(SystemGuardian.class, guardian),
                system.dispatchers().internalDispatcher(), defaultMailbox, rootGuardian,
                new ActorPath(rootPath, "system"), "system", false);
        cell.initChild(systemGuardian);
        systemGuardian.start();

        systemGuardian.sendSystemMessage(new SystemMessage.Watch(guardian, systemGuardian));
        rootGuardian.sendSystemMessage(new SystemMessage.Watch(systemGuardian, rootGuardian));
        system.eventStream().startDefaultLoggers(system);
    }

    private SupervisorStrategy guardianSupervisorStrategy(ActorSystemImpl system) {
        String className = system.settings().GuardianSupervisorStrategyClass;
        try {
            Class<? extends SupervisorStrategyConfigurator> configuratorClass = Class.forName(className, false,
                            system.settings().classLoader())
                    .asSubclass(SupervisorStrategyConfigurator.class);
            var constructor = configuratorClass.getDeclaredConstructor();
            if (!constructor.canAccess(null)) constructor.setAccessible(true);
            return ((SupervisorStrategyConfigurator) constructor.newInstance()).create();
        } catch (InvocationTargetException error) {
            ActorCell.<RuntimeException>throwUnchecked(error.getCause());
            return null;
        } catch (ReflectiveOperationException error) {
            ActorCell.<RuntimeException>throwUnchecked(error);
            return null;
        }
    }

    private SupervisorStrategy rootGuardianStrategy() {
        return new OneForOneStrategy(error -> {
            log.error(error, "guardian failed, shutting down system");
            return SupervisorStrategy.Directive.Stop;
        });
    }

    @Override
    public CompletionStage<Terminated> terminationFuture() {
        return terminationPromise.minimalCompletionStage();
    }

    /**
     * 创建 LocalActorRef，并在其 Mailbox 中放入 Create。
     *
     * <p>按上游顺序先应用部署配置覆盖 Props，再校验并查找 Dispatcher，
     * 最后按选定 Dispatcher 的配置选择 mailbox type。所有配置错误都在创建
     * 引用前以 {@link ConfigurationException} 抛出。</p>
     */
    @Override
    InternalActorRef actorOf(ActorSystemImpl system, Props props, InternalActorRef supervisor,
                              ActorPath path, boolean systemService, Optional<Deploy> deploy,
                              boolean lookupDeploy, boolean async) {
        Props props2 = resolveProps(props, supervisor, lookupDeploy ? deployer.lookup(path) : deploy);

        if (!system.dispatchers().hasDispatcher(props2.dispatcher()))
            throw new ConfigurationException(
                    "Dispatcher [" + props2.dispatcher() + "] not configured for path " + path);
        try {
            MessageDispatcher dispatcher = system.dispatchers().lookup(props2.dispatcher());
            MailboxType mailboxType = system.mailboxes().getMailboxType(props2,
                    system.dispatchers().config(props2.dispatcher()));
            if (async) {
                return new RepointableActorRef(system, props2, dispatcher, mailboxType, supervisor, path)
                        .initialize(true);
            }
            return new LocalActorRef(system, props2, dispatcher, mailboxType, supervisor, path, path.name(), false);
        } catch (RuntimeException error) {
            throw new ConfigurationException(
                    "configuration problem while creating [" + path + "] with dispatcher ["
                            + props2.dispatcher() + "] and mailbox [" + props2.mailbox() + "]", error);
        }
    }

    private Props resolveProps(Props props, InternalActorRef supervisor, Optional<Deploy> deploy) {
        String parentDispatcher = supervisor instanceof ActorRefWithCell withCell && withCell.props() != null
                ? withCell.props().dispatcher() : Deploy.NoDispatcherGiven;
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
    public LocalActorRef rootGuardian() {
        return rootGuardian;
    }

    @Override
    public ActorRef rootGuardianAt(Address address) {
        return getDefaultAddress().equals(address) ? rootGuardian : deadLetters;
    }

    @Override
    public LocalActorRef guardian() {
        return guardian;
    }

    @Override
    public LocalActorRef systemGuardian() {
        return systemGuardian;
    }

    @Override
    public ActorPath rootPath() {
        return rootPath;
    }

    @Override
    public ActorSystem.Settings settings() {
        return system.settings();
    }

    @Override
    public Deployer deployer() {
        return deployer;
    }

    @Override
    public ActorRef deadLetters() {
        return deadLetters;
    }

    @Override
    ActorRef ignoreRef() {
        return ignoreRef;
    }

    @Override
    public ActorRef resolveActorRef(String path) {
        URI uri;
        try {
            uri = new URI(path);
        } catch (URISyntaxException invalidPath) {
            uri = null;
        }
        if (uri != null && getDefaultAddress().equals(ActorPath.address(uri))) {
            String rawPath = uri.getRawPath();
            String fragment = uri.getRawFragment();
            if (fragment != null) rawPath += "#" + fragment;
            List<String> pathElements = rawPath.startsWith("/")
                    ? Arrays.asList(rawPath.substring(1).split("/", -1)) : List.of();
            return resolveActorRef(rootGuardian, pathElements);
        }
        logDeser.debug("Resolve (deserialization) of unknown (invalid) path [{}], using deadLetters.", path);
        return deadLetters;
    }

    @Override
    public ActorRef resolveActorRef(ActorPath path) {
        if (getDefaultAddress().equals(path.address())) return resolveActorRef(rootGuardian, path.elements());
        logDeser.debug("Resolve (deserialization) of foreign path [{}] doesn't match root path [{}], using deadLetters.",
                path, rootPath);
        return deadLetters;
    }

    InternalActorRef resolveActorRef(InternalActorRef ref, List<String> pathElements) {
        if (pathElements.isEmpty()) {
            logDeser.debug("Resolve (deserialization) of empty path doesn't match an active actor, using deadLetters.");
            return deadLetters;
        }
        InternalActorRef resolved = ref.getChild(pathElements.iterator());
        if (resolved != null) return resolved;
        if (log.isDebugEnabled()) {
            logDeser.debug("Resolve (deserialization) of path [{}] doesn't match an active actor. "
                    + "It has probably been stopped, using deadLetters.", String.join("/", pathElements));
        }
        ActorPath path = ref.path();
        for (String element : pathElements) {
            int separator = element.indexOf('#');
            path = separator < 0 ? new ActorPath(path, element)
                    : new ActorPath(path, element.substring(0, separator), Integer.parseInt(element.substring(separator + 1)));
        }
        return new EmptyLocalActorRef(this, path, system.eventStream());
    }

    @Override
    public Optional<Address> getExternalAddressFor(Address address) {
        return getDefaultAddress().equals(address) ? Optional.of(address) : Optional.empty();
    }

    @Override
    public Address getDefaultAddress() {
        return rootPath.address();
    }

    @Override
    Serialization.Information serializationInformation() {
        Serialization.Information cached = serializationInformationCache;
        if (cached != null) return cached;
        if (system.provider() != this) throw new IllegalStateException("Too early access of serializationInformation");
        Serialization.Information information = new Serialization.Information(rootPath.address(), system);
        serializationInformationCache = information;
        return information;
    }

    @Override
    String addressString() {
        return getDefaultAddress().toString();
    }

    @Override
    long systemUid() {
        return systemUid;
    }

    @Override
    public Scheduler scheduler() {
        return system.scheduler();
    }

    @Override
    public ActorPath tempPath(String prefix) {
        StringBuilder name = new StringBuilder(prefix).append('$');
        return new ActorPath(tempNode, Helpers.base64(tempNumber.getAndIncrement(), name));
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

    private static class Guardian extends AbstractActor implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {
        private final SupervisorStrategy supervisorStrategy;

        private Guardian(SupervisorStrategy supervisorStrategy) {
            this.supervisorStrategy = supervisorStrategy;
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return supervisorStrategy;
        }

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
            super(SupervisorStrategy.defaultStrategy);
            this.guardian = guardian;
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
            if (terminating && terminationHooks.isEmpty()) {
                getContext().system().eventStream().stopDefaultLoggers(getContext().system());
                getContext().stop(getSelf());
            }
        }
    }
}
