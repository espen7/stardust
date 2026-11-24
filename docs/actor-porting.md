# Actor porting notes

The implementation follows Pekko commit `85ab376242` and keeps the Pekko class
and method names where Java permits them. The package prefix is
`io.github.espen7.stardust`.

## Creation

`ActorSystem.create` calls `ActorSystemImpl.actorOf`, then
`LocalActorRefProvider.actorOf`, `LocalActorRef`, `ActorCell.init`, and the
`Create` system message. This follows Pekko's `ActorSystem.scala`,
`ActorRefProvider.scala`, `ActorRef.scala`, and `dungeon/Dispatch.scala`.

## Dispatch

`ActorRef.tell` creates an `Envelope`, `Dispatcher.dispatch` enqueues it, and
`Mailbox.run` processes system messages before user messages. The mailbox uses
the same `Open`, `Closed`, `Scheduled`, and suspend bit layout as Pekko.

## Fault handling and termination

`ActorCell.systemInvoke` routes `Recreate`, `Terminate`, `Watch`, `Unwatch`,
and `DeathWatchNotification` to the same lifecycle methods named by Pekko's
`dungeon/FaultHandling.scala` and `dungeon/DeathWatch.scala`.

## Logging

Pekko's `ActorCell` and dungeon collaborators publish structured `Debug`,
`Warning`, and `Error` events to `ActorSystem.eventStream`; the configured
logger handles output. Stardust keeps that boundary with `LogEvent` and
`EventStream`, and installs an SLF4J subscriber without bundling an SLF4J
provider. `ActorSystem.log()` and `AbstractActor.log()` expose the Java logging
facade. Lifecycle messages and debug configuration keys follow Pekko, including
the default-off `pekko.actor.debug.lifecycle` and `pekko.actor.debug.unhandled`.
Events are dispatched in order on a dedicated thread so logging subscribers do
not block Actor mailbox execution. The current bridge writes `[source] message`; backend-specific timestamps,
thread names, and formatting remain under application control. Receive logging,
AutoReceive logging, MDC, and the full Pekko logger actor lifecycle remain
unimplemented.

The corresponding upstream paths are `ActorCell.scala` (`create`, `supervise`,
`autoReceiveMessage`, `publish`), `dungeon/FaultHandling.scala` (`faultRecreate`,
`finishRecreate`, `finishTerminate`), `dungeon/DeathWatch.scala` (`addWatcher`,
`remWatcher`), and `event/Logging.scala` (`LogEvent`, `LoggingAdapter`).

## Java translation choices

Scala `object` entry points are Java static methods. `@tailrec` is a loop.
`Option`, `Future`, and `FiniteDuration` become `Optional`,
`CompletionStage`, and `java.time.Duration`. The Scala `!` entry point is
represented by `tell`, with `sendMessage` retained as the internal method.

The implementation is an independent Java translation. It does not copy
Pekko source files. Pekko is Apache-2.0 licensed. See the upstream source at
https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main.
