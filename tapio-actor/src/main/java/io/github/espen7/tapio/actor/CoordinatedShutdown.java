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

import com.typesafe.config.Config;
import io.github.espen7.tapio.Done;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 按 Pekko 阶段图执行 ActorSystem 关闭任务。
 */
public final class CoordinatedShutdown {
    /** 首个服务解绑前阶段。 */
    public static final String PhaseBeforeServiceUnbind = "before-service-unbind";
    /** 停止接受新的服务请求阶段。 */
    public static final String PhaseServiceUnbind = "service-unbind";
    /** 等待服务请求完成阶段。 */
    public static final String PhaseServiceRequestsDone = "service-requests-done";
    /** 停止服务端点阶段。 */
    public static final String PhaseServiceStop = "service-stop";
    /** 集群关闭前阶段。 */
    public static final String PhaseBeforeClusterShutdown = "before-cluster-shutdown";
    /** Cluster Sharding region 关闭阶段。 */
    public static final String PhaseClusterShardingShutdownRegion = "cluster-sharding-shutdown-region";
    /** 发出 cluster leave 命令阶段。 */
    public static final String PhaseClusterLeave = "cluster-leave";
    /** 关闭 cluster singleton 阶段。 */
    public static final String PhaseClusterExiting = "cluster-exiting";
    /** 等待 cluster exiting 完成阶段。 */
    public static final String PhaseClusterExitingDone = "cluster-exiting-done";
    /** 关闭 cluster extension 阶段。 */
    public static final String PhaseClusterShutdown = "cluster-shutdown";
    /** ActorSystem 终止前阶段。 */
    public static final String PhaseBeforeActorSystemTerminate = "before-actor-system-terminate";
    /** 最后一个 ActorSystem 终止阶段。 */
    public static final String PhaseActorSystemTerminate = "actor-system-terminate";

    /** CoordinatedShutdown 的关闭原因。 */
    public interface Reason {
    }

    /** 未知关闭原因。 */
    public static final class UnknownReason implements Reason {
        private UnknownReason() {
        }
    }

    /** ActorSystem.terminate() 触发的关闭原因。 */
    public static final class ActorSystemTerminateReason implements Reason {
        private ActorSystemTerminateReason() {
        }
    }

    /** JVM 退出触发的关闭原因。 */
    public static final class JvmExitReason implements Reason {
        private JvmExitReason() {
        }
    }

    private static final Reason UNKNOWN_REASON = new UnknownReason();
    private static final Reason ACTOR_SYSTEM_TERMINATE_REASON = new ActorSystemTerminateReason();
    private static final Reason JVM_EXIT_REASON = new JvmExitReason();

    private final ActorSystemImpl system;
    private final Map<String, Phase> phases;
    private final Map<String, CopyOnWriteArrayList<TaskDefinition>> tasks = new ConcurrentHashMap<>();
    private final AtomicReference<Reason> runStarted = new AtomicReference<>();
    private final CompletableFuture<Done> runPromise = new CompletableFuture<>();
    private final Cancellable jvmShutdownHook;

    /**
     * 创建并读取当前 ActorSystem 的关闭阶段配置。
     *
     * @param system 所属 ActorSystem
     */
    public CoordinatedShutdown(ActorSystemImpl system) {
        this.system = system;
        Config config = system.settings().config();
        this.phases = phasesFromConfig(config);
        if (config.getBoolean(ActorPath.Scheme + ".coordinated-shutdown.run-by-jvm-shutdown-hook")) {
            this.jvmShutdownHook = addCancellableJvmShutdownHook(() -> {
                try {
                    runAll(jvmExitReason()).toCompletableFuture().get(
                            config.getDuration(ActorPath.Scheme + ".coordinated-shutdown.close-actor-system-timeout")
                                    .toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException | TimeoutException ignored) {
                    /* JVM shutdown is already in progress; the phase timeout is authoritative. */
                }
            });
        } else {
            this.jvmShutdownHook = cancelledCancellable();
        }
    }

    void cancelJvmShutdownHook() {
        jvmShutdownHook.cancel();
    }

    /**
     * 返回未知关闭原因 singleton。
     *
     * @return 未知关闭原因
     */
    public static Reason unknownReason() {
        return UNKNOWN_REASON;
    }

    /**
     * 返回 ActorSystem.terminate() 关闭原因 singleton。
     *
     * @return ActorSystem terminate 关闭原因
     */
    public static Reason actorSystemTerminateReason() {
        return ACTOR_SYSTEM_TERMINATE_REASON;
    }

    /**
     * 返回 JVM 退出关闭原因 singleton。
     *
     * @return JVM exit 关闭原因
     */
    public static Reason jvmExitReason() {
        return JVM_EXIT_REASON;
    }

    /**
     * 注册可取消的异步关闭任务。
     *
     * @param phase 任务所属阶段
     * @param taskName 任务名称
     * @param task 返回完成 stage 的任务
     * @return 可取消的任务句柄
     * @throws IllegalArgumentException 阶段未知或任务名称为空时抛出
     */
    public Cancellable addCancellableTask(String phase, String taskName,
                                          Supplier<CompletionStage<Done>> task) {
        requirePhaseAndName(phase, taskName);
        Objects.requireNonNull(task, "task");
        TaskDefinition definition = new TaskDefinition(taskName, task);
        tasks.computeIfAbsent(phase, ignored -> new CopyOnWriteArrayList<>()).add(definition);
        return definition;
    }

    /**
     * 注册异步关闭任务。
     *
     * @param phase 任务所属阶段
     * @param taskName 任务名称
     * @param task 返回完成 stage 的任务
     */
    public void addTask(String phase, String taskName, Supplier<CompletionStage<Done>> task) {
        addCancellableTask(phase, taskName, task);
    }

    /**
     * 注册等待 Actor 终止的关闭任务。
     *
     * @param phase 任务所属阶段
     * @param taskName 任务名称
     * @param actor 要停止并等待终止的 Actor
     * @param stopMsg 可选的停止消息
     */
    public void addActorTerminationTask(String phase, String taskName, ActorRef actor,
                                        Optional<Object> stopMsg) {
        addTask(phase, taskName, () -> {
            stopMsg.ifPresent(message -> actor.tell(message, ActorRef.noSender));
            CompletableFuture<Done> result = new CompletableFuture<>();
            pollTermination(actor, result, timeout(phase));
            return result;
        });
    }

    private void pollTermination(ActorRef actor, CompletableFuture<Done> result, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        Runnable[] poll = new Runnable[1];
        poll[0] = () -> {
            if (actor.isTerminated()) {
                result.complete(Done.done());
            } else if (System.nanoTime() - deadline >= 0) {
                result.completeExceptionally(new TimeoutException("Actor [" + actor
                        + "] termination timed out during coordinated shutdown"));
            } else {
                system.scheduler().scheduleOnce(Duration.ofMillis(10), poll[0]);
            }
        };
        system.scheduler().scheduleOnce(Duration.ZERO, poll[0]);
    }

    /**
     * 返回当前已确定的关闭原因。
     *
     * @return 已启动时包含关闭原因，否则为空
     */
    public Optional<Reason> shutdownReason() {
        return Optional.ofNullable(runStarted.get());
    }

    /**
     * Java API：返回当前已确定的关闭原因。
     *
     * @return 已启动时包含关闭原因，否则为空
     */
    public Optional<Reason> getShutdownReason() {
        return shutdownReason();
    }

    /**
     * 注册在 JVM 退出阶段运行的 hook。
     *
     * @param hook JVM 退出时执行的回调
     */
    public void addJvmShutdownHook(Runnable hook) {
        addCancellableJvmShutdownHook(hook);
    }

    /**
     * 注册可取消的 JVM 退出 hook。
     *
     * @param hook JVM 退出时执行的回调
     * @return 可移除该 hook 的句柄
     */
    public Cancellable addCancellableJvmShutdownHook(Runnable hook) {
        if (hook == null) throw new NullPointerException("hook");
        if (runStarted.get() != null) return cancelledCancellable();
        Thread thread = new Thread(hook, system.name() + "-shutdown-hook-" + System.nanoTime());
        try {
            Runtime.getRuntime().addShutdownHook(thread);
        } catch (IllegalStateException alreadyShuttingDown) {
            return cancelledCancellable();
        }
        return new Cancellable() {
            private final AtomicBoolean cancelled = new AtomicBoolean();

            @Override
            public boolean cancel() {
                if (!cancelled.compareAndSet(false, true)) return false;
                try {
                    return Runtime.getRuntime().removeShutdownHook(thread);
                } catch (IllegalStateException alreadyShuttingDown) {
                    return false;
                }
            }

            @Override
            public boolean isCancelled() {
                return cancelled.get();
            }
        };
    }

    private static Cancellable cancelledCancellable() {
        return new Cancellable() {
            @Override
            public boolean cancel() {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return true;
            }
        };
    }

    /**
     * 运行所有配置阶段。
     *
     * @param reason 关闭原因
     * @return 所有阶段完成时的 stage
     */
    public CompletionStage<Done> runAll(Reason reason) {
        return run(reason, Optional.empty());
    }

    /**
     * 从指定阶段开始运行关闭流程。
     *
     * @param reason 关闭原因
     * @param fromPhase 要开始执行的阶段；为空时从第一阶段开始
     * @return 阶段完成时的 stage
     */
    public CompletionStage<Done> run(Reason reason, Optional<String> fromPhase) {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(fromPhase, "fromPhase");
        List<String> ordered = orderedPhases();
        if (fromPhase.isPresent() && !ordered.contains(fromPhase.get())) {
            throw new IllegalArgumentException("Unknown phase [" + fromPhase.get() + "]");
        }
        if (!runStarted.compareAndSet(null, reason)) return runPromise;
        if (fromPhase.isPresent()) {
            int index = ordered.indexOf(fromPhase.get());
            ordered = ordered.subList(index, ordered.size());
        }
        CompletableFuture<Done> chain = CompletableFuture.completedFuture(Done.done());
        for (String phaseName : ordered) {
            String name = phaseName;
            chain = chain.thenCompose(ignored -> runPhase(name));
        }
        chain.whenComplete((value, error) -> {
            if (error == null) runPromise.complete(Done.done());
            else runPromise.completeExceptionally(error);
        });
        return runPromise;
    }

    /**
     * 返回配置阶段的超时时间。
     *
     * @param phase 阶段名称
     * @return 阶段超时时间
     * @throws IllegalArgumentException 阶段未知时抛出
     */
    public Duration timeout(String phase) {
        Phase value = phases.get(phase);
        if (value == null) throw new IllegalArgumentException("Unknown phase [" + phase + "]");
        return value.timeout;
    }

    /**
     * 返回已注册任务阶段的超时总和。
     *
     * @return 已注册任务所属阶段的超时时间总和
     */
    public Duration totalTimeout() {
        Duration total = Duration.ZERO;
        for (String phase : tasks.keySet()) total = total.plus(timeout(phase));
        return total;
    }

    private CompletionStage<Done> runPhase(String name) {
        Phase phase = phases.get(name);
        if (phase == null || !phase.enabled) return CompletableFuture.completedFuture(Done.done());
        List<TaskDefinition> definitions = tasks.getOrDefault(name, new CopyOnWriteArrayList<>());
        List<CompletableFuture<Done>> futures = new ArrayList<>();
        for (TaskDefinition task : definitions) futures.add(runTask(task, phase.recover));
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
        CompletableFuture<Done> result = all.thenApply(ignored -> Done.done());
        return withTimeout(result, phase.timeout, phase.recover, name);
    }

    private CompletableFuture<Done> runTask(TaskDefinition task, boolean recover) {
        if (!task.started.compareAndSet(false, true) || task.cancelled.get()) {
            return CompletableFuture.completedFuture(Done.done());
        }
        try {
            CompletionStage<Done> stage = task.task.get();
            CompletableFuture<Done> future = stage.toCompletableFuture();
            if (!recover) return future;
            return future.handle((value, error) -> Done.done()).toCompletableFuture();
        } catch (Throwable error) {
            if (recover) return CompletableFuture.completedFuture(Done.done());
            return CompletableFuture.failedFuture(error);
        }
    }

    private CompletableFuture<Done> withTimeout(CompletableFuture<Done> value, Duration timeout,
                                                boolean recover, String phase) {
        CompletableFuture<Done> timeoutFuture = new CompletableFuture<>();
        Cancellable timer = system.scheduler().scheduleOnce(timeout, () -> {
            if (recover) timeoutFuture.complete(Done.done());
            else timeoutFuture.completeExceptionally(new TimeoutException("Coordinated shutdown phase ["
                    + phase + "] timed out after " + timeout));
        });
        CompletableFuture<Done> result = new CompletableFuture<>();
        value.whenComplete((done, error) -> {
            if (error == null) result.complete(done);
            else result.completeExceptionally(error);
        });
        timeoutFuture.whenComplete((done, error) -> {
            if (error == null) result.complete(done);
            else result.completeExceptionally(error);
        });
        result.whenComplete((done, error) -> timer.cancel());
        return result;
    }

    private void requirePhaseAndName(String phase, String taskName) {
        if (!phases.containsKey(phase)) throw new IllegalArgumentException("Unknown phase [" + phase + "]");
        if (taskName == null || taskName.isEmpty()) throw new IllegalArgumentException("Task name must not be empty");
    }

    private List<String> orderedPhases() {
        List<String> result = new ArrayList<>();
        Set<String> visiting = ConcurrentHashMap.newKeySet();
        Set<String> visited = ConcurrentHashMap.newKeySet();
        for (String name : phases.keySet()) visit(name, visiting, visited, result);
        return result;
    }

    private void visit(String name, Set<String> visiting, Set<String> visited, List<String> result) {
        if (visited.contains(name)) return;
        if (!visiting.add(name)) throw new IllegalArgumentException("Cycle detected in CoordinatedShutdown phases");
        Phase phase = phases.get(name);
        if (phase != null) for (String dependency : phase.dependsOn) visit(dependency, visiting, visited, result);
        visiting.remove(name);
        visited.add(name);
        result.add(name);
    }

    private static Map<String, Phase> phasesFromConfig(Config config) {
        Config phaseConfig = config.getConfig(ActorPath.Scheme + ".coordinated-shutdown.phases");
        Duration defaultTimeout = config.getDuration(ActorPath.Scheme + ".coordinated-shutdown.default-phase-timeout");
        Map<String, Phase> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : phaseConfig.root().unwrapped().entrySet()) {
            Config current = phaseConfig.getConfig(entry.getKey());
            Duration timeout = current.hasPath("timeout") ? current.getDuration("timeout") : defaultTimeout;
            List<String> dependencies = current.hasPath("depends-on")
                    ? current.getStringList("depends-on") : List.of();
            boolean recover = !current.hasPath("recover") || current.getBoolean("recover");
            boolean enabled = !current.hasPath("enabled") || current.getBoolean("enabled");
            result.put(entry.getKey(), new Phase(timeout, dependencies, recover, enabled));
        }
        return Collections.unmodifiableMap(result);
    }

    private record Phase(Duration timeout, List<String> dependsOn, boolean recover, boolean enabled) {
    }

    private static final class TaskDefinition implements Cancellable {
        private final String name;
        private final Supplier<CompletionStage<Done>> task;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        private TaskDefinition(String name, Supplier<CompletionStage<Done>> task) {
            this.name = name;
            this.task = task;
        }

        @Override
        public boolean cancel() {
            return !started.get() && cancelled.compareAndSet(false, true);
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }
    }
}
