# Tapio

Tapio is a Java 25 project that reads and incrementally ports the Apache Pekko Classic Actor local runtime. The implementation follows [Pekko commit `85ab376242`](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main), keeping upstream class names, method names, and call order as closely as possible so the two codebases can be read side by side.

Tapio is in an early porting stage. It is intended for source study and continued implementation, and does not provide the full feature set of Pekko.

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## Scope

The current module is `tapio-actor`. It includes local Actor creation, message envelopes, Mailbox and Dispatcher support, basic lifecycle handling, basic One-for-One supervision, DeathWatch messages, `Patterns.ask`, one-shot scheduling, and initial logging event support.

The following areas still differ from upstream:

- `ChildrenContainer` is split into Pekko-style empty, normal, waiting-for-termination, and terminated states, and lifecycle handling continues after restart and termination. `ChildRestartStats` tracks child UIDs, retry counts, and time windows.
- The supervision path supports Pekko's default exception classification, One-for-One and All-for-One supervision, retry limits, escalation, and failure logging. The complete guardian supervision lifecycle is not yet ported.
- One-shot scheduling uses `LightArrayRevolverScheduler` and is configured under `tapio.scheduler`. Periodic scheduling APIs are not yet ported.
- Local `Patterns.ask` uses the system Scheduler and temporary `PromiseActorRef` instances. It supports Classic `Status.Success` and `Status.Failure`, DeathWatch for temporary references, dead letters for late replies, and a millisecond overload returning a Java `CompletionStage`.
- EventStream uses ActorRef subscribers and class channels, dispatching according to parent and child channel types. The default dead-letter logger supports counting and pausing. The system guardian, `systemActorOf`, `CoordinatedShutdown` phase termination, the JVM shutdown hook, and the `LogMarker`/MDC bridge are included in the local runtime. `System.exit`, cluster tasks, and the complete logging service are not yet ported. Receive timeout, ActorSelection, Stash, routing, serialization, I/O, Typed, Remote, and Sharding are not yet implemented.

Source locations and Java counterparts for each upstream concept are recorded in [docs/actor-porting.md](docs/actor-porting.md).

## Requirements

- JDK 25
- Maven 3.9 or later

There is no Maven Wrapper in this repository. After checking `java -version` and `mvn -version`, run from the repository root:

```shell
mvn test
```

The unit tests cover basic local-runtime call paths and are not a substitute for the Pekko test suite. To package the module without tests, run `mvn -DskipTests package`.

## ActorSystem Configuration

`ActorSystem.create(name)` loads `application.conf` and `reference.conf`. `ActorSystem.create(name, config)` uses the supplied configuration and fills in defaults from `reference.conf`. Effective settings can be read through `system.settings().config()`.

`tapio.actor.default-dispatcher.throughput` defaults to `5` and controls how many ordinary messages a Mailbox processes in one scheduling batch.
`tapio.actor.default-dispatcher.throughput-deadline-time` defaults to `0ms`, which disables the batch time limit.
`system.dispatchers().lookup(id)` supports named ordinary Dispatchers; multiple aliases can point to the same instance. Actor creation currently uses the default Dispatcher.

## Logging

`ActorSystem` and `AbstractActor` expose `log()` with `debug`, `info`, `warning`, `error`, and `{}` template methods. Log calls first become `LogEvent` instances containing the source path, source class, level, message, and cause, then are published to `ActorSystem.eventStream()`. The default SLF4J logger Actor prefixes messages with the source path; applications can subscribe to structured events with an ActorRef and `LogEvent.class`.

The runtime depends only on `slf4j-api` and does not bundle a logging implementation. Applications must add an SLF4J provider at runtime, such as Logback, Log4j2, SLF4J Simple, or a JUL provider, and configure its levels and output format. Without a provider, SLF4J reports the missing provider and uses a NOP logger.

ActorSystem reads Tapio logging settings from `application.conf`:

```hocon
tapio.loglevel = "DEBUG"
tapio.actor.debug.lifecycle = on
tapio.actor.debug.unhandled = on
```

To emit lifecycle and unhandled-message DEBUG logs, set `tapio.loglevel` to `DEBUG` and enable DEBUG for the relevant logger in the selected SLF4J provider. `reference.conf` uses `tapio.actor.loglevel` as the fallback value for `tapio.loglevel`. Lifecycle logs cover Actor creation, supervision, restart, stop, and DeathWatch changes, and are disabled by default. Unhandled messages are always published as `UnhandledMessage`; they become DEBUG logs when `debug.unhandled = on`. The `debug.receive` and `debug.autoreceive` keys are retained, but the related receive decorators and AutoReceive API are not yet ported.

## Module Layout

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem, ActorRef, ActorCell, and lifecycle
    │   ├── dispatch/    # Dispatcher, Mailbox, Envelope, and SystemMessage
    │   ├── event/       # LogEvent, EventStream, LoggingAdapter, and SLF4J bridge
    │   ├── japi/        # Java Creator and ReceiveBuilder
    │   └── pattern/     # ask pattern
    └── resources/
        └── reference.conf
```

Java packages follow Pekko's relative hierarchy and use `io.github.espen7.tapio` as the package prefix. Entry points and class mappings are based on Pekko commit `85ab376242`.

## License

Tapio is released under the Apache License 2.0; see [LICENSE](LICENSE). Upstream references should continue to be recorded, and applicable copyright, license, and NOTICE information must be retained for code directly derived from or copied from Pekko.
