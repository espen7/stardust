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
| Ordinary Dispatcher lifecycle, lazy executor recreation, inhabitants, and idle shutdown | Available in `tapio-actor` |
| Props and deployment binding of Dispatcher/Mailbox, unbounded and bounded mailboxes | Available in `tapio-actor` |
| EventStream, structured logging, local ActorSelection, `ask`, and one-shot and periodic scheduling | Available in `tapio-actor` |
| Routing | Planned |
| Cluster Sharding | Planned |
| Kubernetes-based cluster discovery | Planned |
| The transport and membership pieces required by the cluster features | Planned as focused Tapio components |

`Available` means the local runtime contract is implemented and covered by Tapio tests. It does not mean full Pekko source parity or that the complete Pekko test suite has been run. Internal and PinnedDispatchers, executor configuration, advanced mailboxes, and the complete logging lifecycle remain incomplete.

The initial scope does not include the complete Akka/Pekko ecosystem: Typed APIs, Streams, Persistence, broad serialization integrations, full remoting compatibility, or unrelated enterprise integrations. The scope can expand when a component directly supports the actor platform and its target workloads.

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

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem, ActorRef, lifecycle, supervision
    │   ├── dispatch/    # Dispatcher, Mailbox, Envelope, system messages
    │   ├── event/       # EventStream, LogEvent, SLF4J bridge
    │   ├── japi/        # Java Creator and ReceiveBuilder APIs
    │   └── pattern/     # ask pattern
    └── resources/       # reference.conf defaults
```

`tapio-actor` currently provides the local runtime. Routing, sharding, and Kubernetes discovery will be added as focused modules around the same actor model instead of requiring the full Pekko ecosystem.

## Configuration and Logging

`ActorSystem.create(name)` loads `application.conf` and the module defaults from `reference.conf`. The runtime exposes named Dispatcher lookup, Scheduler settings, coordinated shutdown, dead-letter logging, and SLF4J-backed structured events through `ActorSystem`.

Tapio depends on `slf4j-api` only. Applications choose and configure a provider such as Logback, Log4j2, SLF4J Simple, or a JUL provider.

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
