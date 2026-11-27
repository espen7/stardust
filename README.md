# Tapio

**A lightweight Java 25 actor platform for high-concurrency distributed systems.**

[![Java 25](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.org/projects/jdk/25/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Tapio is a Java-first rewrite of the Classic Actor model behind Akka 2.6 and Apache Pekko. It brings the actor model to modern Java, with a small runtime, a virtual-thread-oriented concurrency design, and a focused set of distributed building blocks for games and other high-concurrency services.

Tapio is intended to be easy to learn, easy to embed, and easy for Java developers to read from the public API down to the runtime implementation. It keeps the useful parts of the Akka ecosystem while leaving out the large set of integrations and abstractions that are not needed by every service.

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## Akka, Pekko, and Tapio

Akka 2.6 is the API and design lineage for the Classic Actor model. Apache Pekko is the Apache-2.0 community fork and continuation of Akka after the project changed direction. Tapio is an independent Java implementation inspired by that lineage:

- familiar actor concepts such as `ActorSystem`, `ActorRef`, Mailbox, supervision, EventStream, and `ask`;
- Java 25 APIs and a runtime designed for modern concurrency, including virtual threads;
- a deliberately smaller scope instead of a drop-in copy of the full Akka or Pekko ecosystem.

Tapio is not affiliated with Lightbend or the Apache Pekko project, and it is not a binary-compatible replacement for Akka or Pekko.

## Why Tapio

- **Java-native**: no Scala toolchain is required for application code or runtime development.
- **Modern concurrency**: Java 25 is the baseline, with virtual threads as a core direction for scalable workloads.
- **Small surface area**: keep the actor runtime and the distributed components that are useful in real services.
- **Readable implementation**: clear Java code and a familiar Classic Actor model make source analysis practical.
- **High-concurrency focus**: a foundation for game servers, gateways, simulation services, and other stateful workloads.

## Scope and Status

Tapio is in early development. Public APIs and configuration may change before the first stable release.

| Area | Status |
| --- | --- |
| Local Classic Actor runtime | Available in `tapio-actor` |
| Actor lifecycle, Mailbox, supervision (including guardian strategy configuration), and DeathWatch | Available in `tapio-actor` |
| PoisonPill, Kill, ReceiveTimeout, `watchWith`, and restart lifecycle handling | Available in `tapio-actor` |
| Ordinary Dispatcher lifecycle, nested task batching, lazy executor recreation, inhabitants, and idle shutdown | Available in `tapio-actor` |
| Internal Dispatcher for guardians and PinnedDispatcher for dedicated single-actor execution | Available in `tapio-actor` |
| Built-in `default-executor`, `fork-join-executor`, `thread-pool-executor`, and `virtual-thread-executor` (plus `virtualize = on`) configuration | Available in `tapio-actor` |
| Props and deployment binding of Dispatcher/Mailbox, unbounded/bounded/deque/priority/control-aware mailboxes | Available in `tapio-actor` |
| EventStream with automatic unsubscription on subscriber termination, structured logging, `LoggingReceive`, `LoggingFilter`, `log-config-on-start`, default logger startup/shutdown and dynamic levels, local ActorSelection, `ask`/`pipe`/`gracefulStop`/`BackoffSupervisor`/`CircuitBreaker`, `Timers`, `Stash`, `AbstractFSM`, `Extension`/`DynamicAccess`, and one-shot and periodic scheduling | Available in `tapio-actor` |
| Routing | Planned |
| Classic Remoting (Netty 4.2 TCP, Phi Accrual Failure Detector, RemoteWatcher, Serialization) | Available in `tapio-remote` |
| Cluster Membership, Gossip, Reachability, and Split Brain Resolver | Available in `tapio-cluster` |
| Cluster Singleton (`ClusterSingletonManager` / `ClusterSingletonProxy`) | Available in `tapio-cluster-tools` |
| Distributed Data (`Replicator`, `ORSet`, `LWWRegister`, `VersionVector`) | Available in `tapio-distributed-data` |
| Cluster Sharding (`ddata` mode, `LeastShardAllocationStrategy`, `Passivate`, `remember-entities`) | Available in `tapio-cluster-sharding` |
| Distributed Lease SPI (`Lease`, `LeaseProvider`, SBR `lease-majority`, Singleton/Sharding `use-lease`) | Available in `tapio-coordination` |
| Service Discovery (`config`, `aggregate`) | Available in `tapio-discovery` |
| Kubernetes-based cluster discovery & Bootstrap | Planned |

`Available` means the runtime contract is implemented and covered by Tapio tests. It does not mean full Pekko source parity or that the complete Pekko test suite has been run. Affinity/custom executors remain incomplete.

The scope does not include the complete Akka/Pekko ecosystem: **Typed Actors (Pekko Typed), Persistence, Streams, and Artery are explicitly excluded**, as are routing and unrelated enterprise integrations. The scope can expand when a component directly supports the actor platform and its target workloads.

## Quick Start

### Requirements

- JDK 25
- Maven 3.9 or later

### Build from source

```shell
git clone https://github.com/espen7/tapio.git
cd tapio
mvn test
```

To package the actor module without running tests:

```shell
mvn -DskipTests package
```

### Minimal actor

```java
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.pattern.Patterns;

import java.time.Duration;

public final class EchoActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, message -> getSender().tell(message, getSelf()))
                .build();
    }

    public static void main(String[] args) {
        try (ActorSystem system = ActorSystem.create("example")) {
            ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");
            Object reply = Patterns.ask(echo, "hello", Duration.ofSeconds(1))
                    .toCompletableFuture()
                    .join();
            System.out.println(reply);
        }
    }
}
```

The current repository is built from source. Published artifact coordinates will be documented when the first release is available.

## Architecture

The local actor tree starts at the root guardian (`/`). Its `/user` and `/system`
children supervise application and system actors. The root also resolves `/temp`
for temporary references and `/deadLetters` for undeliverable messages.

`ActorSystem` and `ActorContext` implement `ActorRefFactory` for named and
automatically named actor creation, local ActorSelection, and asynchronous stop.
Actors attached through the system buffer messages until their guardian activates the mailbox.
Use `ActorSystem.stop(ref)` or `getContext().stop(ref)` to stop an actor.
Java actors use `AbstractActor.ActorContext` and can compose handlers with `Receive.orElse`.
The `preRestart` hook receives the failed message as an `Optional<Object>`.

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem, ActorRef, lifecycle, supervision
    │   ├── dispatch/    # Dispatcher, Mailbox, Envelope, system messages
    │   ├── event/       # EventStream, LogEvent, default loggers, SLF4J bridge
    │   ├── japi/        # Java Creator and ReceiveBuilder APIs
    │   └── pattern/     # ask pattern
    └── resources/       # reference.conf defaults
```

`tapio-actor` currently provides the local runtime. Routing, sharding, and Kubernetes discovery will be added as focused modules around the same actor model instead of requiring the full Pekko ecosystem.

## Configuration and Logging

`ActorSystem.create(name)` loads `application.conf` and the module defaults from `reference.conf`. The runtime exposes named Dispatcher lookup, an internal Dispatcher for guardians, PinnedDispatcher configuration, built-in fork-join/thread-pool executor configuration, a shared thread factory, Scheduler settings, coordinated shutdown, dead-letter logging, and structured events through `ActorSystem`.

Logging starts with a synchronous stdout logger, then initializes the actors configured by `tapio.loggers` on `tapio.loggers-dispatcher` with the bounded `tapio.logger-startup-timeout` handshake. Logger names use monotonically increasing IDs per system, including when the same logger class is configured more than once. The defaults are `DefaultLogger`, the default Dispatcher, and `5s`; `tapio.stdout-loglevel` defaults to `WARNING`, while `tapio.loglevel` controls the configured loggers. `EventStream.setLogLevel` updates their level subscriptions, and shutdown unsubscribes and stops them after termination hooks complete. The default logger mailbox drains eligible queued events to stdout before termination.

Tapio depends on `slf4j-api` only and does not bundle a provider. To bridge events to SLF4J, explicitly configure `tapio.loggers = ["io.github.espen7.tapio.event.Slf4jLogger"]` and choose a provider such as Logback, Log4j2, SLF4J Simple, or JUL. The bridge uses the logger mailbox, writes marker properties, Pekko metadata, and event MDC in that order, preserves the original message text, and clears MDC after each call. The system UID is resolved lazily and omitted from MDC while unavailable; timestamp formatting remains overridable in both the stdout and SLF4J loggers.

Detailed configuration notes and source-level design records are available in [docs/actor-porting.md](docs/actor-porting.md).

## Roadmap

1. Complete and stabilize the local actor runtime.
2. Add routing primitives for common service and game-server topologies.
3. Add cluster membership and Kubernetes discovery.
4. Add Cluster Sharding for stateful, horizontally scaled actors.
5. Validate the Java 25 and virtual-thread execution model under representative high-concurrency workloads.

## Contributing

Issues and pull requests are welcome. For substantial API or runtime changes, open an issue first so the behavior and compatibility impact can be discussed. New runtime behavior should include focused JUnit 5 coverage and documentation when it changes the public contract.

## Source Lineage and License

Tapio is released under the [Apache License 2.0](LICENSE). The actor runtime follows Classic Actor semantics and uses Apache Pekko as an implementation reference. Source-level mappings, adaptation notes, and applicable upstream notices are recorded in [docs/actor-porting.md](docs/actor-porting.md) and in source headers.

Tapio is an independent project and is not endorsed by or affiliated with Lightbend, Akka, or Apache Pekko.
