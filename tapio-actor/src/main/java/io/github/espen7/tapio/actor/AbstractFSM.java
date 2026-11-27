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

import io.github.espen7.tapio.event.Warning;
import io.github.espen7.tapio.japi.pf.FSMStateFunctionBuilder;
import io.github.espen7.tapio.japi.pf.FSMTransitionHandlerBuilder;
import io.github.espen7.tapio.japi.pf.PartialFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 有限状态机（FSM）Actor 抽象基类。
 *
 * @param <S> 状态类型
 * @param <D> 状态数据类型
 */
public abstract class AbstractFSM<S, D> extends AbstractActor {
    private record TimeoutMarker(long generation) {
    }

    private record ListenerTerminated(ActorRef actorRef) {
    }

    private enum TimerMode {
        SINGLE(false),
        FIXED_DELAY(true),
        FIXED_RATE(true);

        private final boolean repeat;

        TimerMode(boolean repeat) {
            this.repeat = repeat;
        }
    }

    private record NamedTimerMsg(String name, Object msg, TimerMode mode, int generation, AbstractFSM<?, ?> owner) {
    }

    private static final class NamedTimer {
        private final Cancellable task;
        private final int generation;

        private NamedTimer(Cancellable task, int generation) {
            this.task = task;
            this.generation = generation;
        }

        private void cancel() {
            task.cancel();
        }
    }

    private final Map<S, PartialFunction<FSM.Event<D>, FSM.State<S, D>>> stateFunctions = new HashMap<>();
    private final Map<S, Duration> stateTimeouts = new HashMap<>();
    private final List<BiConsumer<S, S>> transitionHandlers = new ArrayList<>();
    private final Set<ActorRef> listeners = new LinkedHashSet<>();
    private final Set<ActorRef> watchedListeners = new HashSet<>();
    private final Map<String, NamedTimer> timers = new HashMap<>();
    private int timerGeneration;

    private PartialFunction<FSM.Event<D>, FSM.State<S, D>> handleEvent;
    private Consumer<FSM.StopEvent<S, D>> terminateEvent = ignored -> {
    };
    private FSM.State<S, D> currentState;
    private FSM.State<S, D> nextState;
    private long timeoutGeneration = 0L;
    private Cancellable timeoutTask;

    /** 创建 FSM Actor 实例。 */
    protected AbstractFSM() {
    }

    /** 创建状态函数构建器。 */
    protected final FSMStateFunctionBuilder<S, D> matchEvent(
            Class<?> eventType, java.util.function.BiFunction<Object, D, FSM.State<S, D>> apply) {
        return new FSMStateFunctionBuilder<S, D>().matchEvent(Object.class, eventType::isInstance, apply);
    }

    /** 创建按事件值 {@code equals} 匹配的状态函数构建器。 */
    protected final FSMStateFunctionBuilder<S, D> matchEventEquals(
            Object event, java.util.function.BiFunction<Object, D, FSM.State<S, D>> apply) {
        return new FSMStateFunctionBuilder<S, D>().matchEventEquals(event, apply);
    }

    /** 返回新的 {@link FSMStateFunctionBuilder}。 */
    protected final FSMStateFunctionBuilder<S, D> stateFunction() {
        return new FSMStateFunctionBuilder<>();
    }

    /** 返回新的 {@link FSMTransitionHandlerBuilder}。 */
    protected final FSMTransitionHandlerBuilder<S> transitionHandler() {
        return new FSMTransitionHandlerBuilder<>();
    }

    /**
     * 设置初始状态与初始数据，需在构造器中 {@link #initialize()} 之前调用。
     */
    protected final void startWith(S stateName, D stateData) {
        startWith(stateName, stateData, null);
    }

    /**
     * 设置初始状态、初始数据与初始超时。
     */
    protected final void startWith(S stateName, D stateData, Duration timeout) {
        this.currentState = new FSM.State<>(stateName, stateData, timeout, null, List.of(), true);
    }

    /**
     * 注册指定状态的消息处理函数。
     */
    protected final void when(S stateName, FSMStateFunctionBuilder<S, D> builder) {
        when(stateName, null, builder);
    }

    /**
     * 注册指定状态的消息处理函数及默认状态超时。
     */
    protected final void when(S stateName, Duration stateTimeout, FSMStateFunctionBuilder<S, D> builder) {
        Objects.requireNonNull(builder, "builder must not be null");
        stateTimeouts.put(stateName, stateTimeout);
        PartialFunction<FSM.Event<D>, FSM.State<S, D>> fn = builder.build();
        stateFunctions.merge(stateName, fn, PartialFunction::orElse);
    }

    /**
     * 注册未匹配当前状态规则时的兜底处理函数。
     */
    protected final void whenUnhandled(FSMStateFunctionBuilder<S, D> builder) {
        this.handleEvent = Objects.requireNonNull(builder, "builder must not be null").build();
    }

    /**
     * 注册状态转换监听器。
     */
    protected final void onTransition(FSMTransitionHandlerBuilder<S> builder) {
        transitionHandlers.add(Objects.requireNonNull(builder, "builder must not be null").build());
    }

    /**
     * 注册状态转换监听器。
     */
    protected final void onTransition(BiConsumer<S, S> handler) {
        transitionHandlers.add(Objects.requireNonNull(handler, "handler must not be null"));
    }

    /**
     * 注册 FSM 终止回调。
     */
    protected final void onTermination(Consumer<FSM.StopEvent<S, D>> handler) {
        this.terminateEvent = Objects.requireNonNull(handler, "handler must not be null");
    }

    /** 返回当前状态名。 */
    public final S stateName() {
        return currentState.stateName();
    }

    /** 返回当前状态数据。 */
    public final D stateData() {
        return currentState.stateData();
    }

    /** 在 {@code onTransition} 回调中返回即将进入的状态数据。 */
    public final D nextStateData() {
        if (nextState == null) {
            throw new IllegalStateException("nextStateData is only available during onTransition");
        }
        return nextState.stateData();
    }

    /** 转换到指定目标状态。 */
    public final FSM.State<S, D> goTo(S nextStateName) {
        return new FSM.State<>(nextStateName, currentState.stateData(), null, null, List.of(), true);
    }

    /** 保持当前状态。 */
    public final FSM.State<S, D> stay() {
        return goTo(currentState.stateName()).withNotifying(false);
    }

    /** 以 {@link FSM.Normal} 原因停止 FSM Actor。 */
    public final FSM.State<S, D> stop() {
        return stop(FSM.Normal.INSTANCE);
    }

    /** 以指定原因停止 FSM Actor。 */
    public final FSM.State<S, D> stop(FSM.Reason reason) {
        return stop(reason, currentState.stateData());
    }

    /** 以指定原因和最终状态数据停止 FSM Actor。 */
    public final FSM.State<S, D> stop(FSM.Reason reason, D stateData) {
        return stay().using(stateData).withStopReason(reason);
    }

    /** 完成初始状态激活并启动初始状态超时（若已配置）。 */
    protected final void initialize() {
        if (currentState == null) {
            throw new IllegalStateException("startWith must be called before initialize()");
        }
        makeTransition(currentState);
    }

    /**
     * 显式设置或更新指定状态的默认超时时间。
     *
     * @param state 目标状态
     * @param timeout 超时时间；{@code null} 表示移除默认超时
     */
    public final void setStateTimeout(S state, Duration timeout) {
        if (timeout == null) {
            stateTimeouts.remove(state);
        } else {
            stateTimeouts.put(state, timeout);
        }
    }

    /**
     * 启动单次命名定时器，到期后向当前 FSM 发送 {@code msg}。
     *
     * @param name 定时器名称
     * @param msg 到期消息
     * @param delay 延迟时长
     */
    public final void startSingleTimer(String name, Object msg, Duration delay) {
        startNamedTimer(name, msg, delay, TimerMode.SINGLE);
    }

    /**
     * 启动固定延迟命名定时器，周期性向当前 FSM 发送 {@code msg}。
     *
     * @param name 定时器名称
     * @param msg 定时消息
     * @param delay 固定延迟
     */
    public final void startTimerWithFixedDelay(String name, Object msg, Duration delay) {
        startNamedTimer(name, msg, delay, TimerMode.FIXED_DELAY);
    }

    /**
     * 启动固定频率命名定时器，按固定周期向当前 FSM 发送 {@code msg}。
     *
     * @param name 定时器名称
     * @param msg 定时消息
     * @param interval 固定周期
     */
    public final void startTimerAtFixedRate(String name, Object msg, Duration interval) {
        startNamedTimer(name, msg, interval, TimerMode.FIXED_RATE);
    }

    private void startNamedTimer(String name, Object msg, Duration timeout, TimerMode mode) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(msg, "msg");
        Objects.requireNonNull(timeout, "timeout");
        NamedTimer previous = timers.get(name);
        if (previous != null) {
            previous.cancel();
        }
        int gen = ++timerGeneration;
        NamedTimerMsg timerMsg = new NamedTimerMsg(name, msg, mode, gen, this);
        Scheduler scheduler = getContext().system().scheduler();
        Cancellable task = switch (mode) {
            case SINGLE -> scheduler.scheduleOnce(timeout, getSelf(), timerMsg, getContext().dispatcher(), getSelf());
            case FIXED_DELAY -> scheduler.scheduleWithFixedDelay(
                    timeout, timeout, getSelf(), timerMsg, getContext().dispatcher(), getSelf());
            case FIXED_RATE -> scheduler.scheduleAtFixedRate(
                    timeout, timeout, getSelf(), timerMsg, getContext().dispatcher(), getSelf());
        };
        timers.put(name, new NamedTimer(task, gen));
    }

    /**
     * 取消指定名称的定时器。
     *
     * @param name 定时器名称
     */
    public final void cancelTimer(String name) {
        NamedTimer existing = timers.remove(name);
        if (existing != null) {
            existing.cancel();
        }
    }

    /**
     * 查询指定名称的定时器当前是否处于激活状态。
     *
     * @param name 定时器名称
     * @return 是否激活
     */
    public final boolean isTimerActive(String name) {
        return timers.containsKey(name);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TimeoutMarker.class, marker -> {
                    if (marker.generation() == timeoutGeneration) {
                        processMsg(FSM.StateTimeout);
                    }
                })
                .match(NamedTimerMsg.class, timerMsg -> {
                    NamedTimer active = timers.get(timerMsg.name());
                    if (timerMsg.owner() == this && active != null && active.generation == timerMsg.generation()) {
                        if (!timerMsg.mode().repeat) {
                            timers.remove(timerMsg.name());
                        }
                        processMsg(timerMsg.msg());
                    }
                })
                .match(FSM.SubscribeTransitionCallBack.class, sub -> {
                    addListener(sub.actorRef());
                    sub.actorRef().tell(new FSM.CurrentState<>(getSelf(), currentState.stateName()), getSelf());
                })
                .match(FSM.UnsubscribeTransitionCallBack.class, unsub -> removeListener(unsub.actorRef()))
                .match(ListenerTerminated.class, term -> {
                    listeners.remove(term.actorRef());
                    watchedListeners.remove(term.actorRef());
                })
                .matchAny(this::processMsg)
                .build();
    }

    private void addListener(ActorRef actorRef) {
        if (actorRef == null) return;
        listeners.add(actorRef);
        if (watchedListeners.add(actorRef)) {
            getContext().watchWith(actorRef, new ListenerTerminated(actorRef));
        }
    }

    private void removeListener(ActorRef actorRef) {
        listeners.remove(actorRef);
        if (watchedListeners.remove(actorRef)) {
            getContext().unwatch(actorRef);
        }
    }

    private void processMsg(Object msg) throws Exception {
        FSM.Event<D> event = new FSM.Event<>(msg, currentState.stateData());
        PartialFunction<FSM.Event<D>, FSM.State<S, D>> stateFn = stateFunctions.get(currentState.stateName());
        FSM.State<S, D> next;
        if (stateFn != null && stateFn.isDefinedAt(event)) {
            next = stateFn.apply(event);
        } else if (handleEvent != null && handleEvent.isDefinedAt(event)) {
            next = handleEvent.apply(event);
        } else {
            getContext().getSystem().eventStream().publish(
                    new Warning(getSelf().path().toString(), getClass(),
                            "unhandled event " + msg + " in state " + currentState.stateName()));
            next = stay();
        }
        applyState(next);
    }

    private void applyState(FSM.State<S, D> next) {
        for (int i = next.replies().size() - 1; i >= 0; i--) {
            getSender().tell(next.replies().get(i), getSelf());
        }
        if (next.stopReason() != null) {
            terminate(next);
            getContext().stop(getSelf());
        } else {
            makeTransition(next);
        }
    }

    private void makeTransition(FSM.State<S, D> next) {
        if (!stateFunctions.containsKey(next.stateName())) {
            terminate(stay().withStopReason(
                    new FSM.Failure("Next state " + next.stateName() + " does not exist")));
            getContext().stop(getSelf());
            return;
        }
        cancelTimeout();
        S oldStateName = currentState.stateName();
        this.nextState = next;
        try {
            if (!Objects.equals(oldStateName, next.stateName()) || next.notifying()) {
                for (BiConsumer<S, S> handler : transitionHandlers) {
                    handler.accept(oldStateName, next.stateName());
                }
                if (!listeners.isEmpty()) {
                    FSM.Transition<S> transition = new FSM.Transition<>(getSelf(), oldStateName, next.stateName());
                    for (ActorRef listener : List.copyOf(listeners)) {
                        listener.tell(transition, getSelf());
                    }
                }
            }
            this.currentState = next;
        } finally {
            this.nextState = null;
        }
        Duration timeout = next.timeoutOverridden() ? next.timeout() : stateTimeouts.get(next.stateName());
        if (timeout != null && !timeout.isNegative()) {
            long gen = ++timeoutGeneration;
            timeoutTask = getContext().system().scheduler().scheduleOnce(
                    timeout, getSelf(), new TimeoutMarker(gen), getContext().dispatcher(), getSelf());
        }
    }

    private void cancelTimeout() {
        timeoutGeneration++;
        if (timeoutTask != null) {
            timeoutTask.cancel();
            timeoutTask = null;
        }
    }

    private void terminate(FSM.State<S, D> next) {
        cancelTimeout();
        if (currentState.stopReason() == null) {
            logTermination(next.stopReason());
            currentState = next;
            terminateEvent.accept(new FSM.StopEvent<>(next.stopReason(), next.stateName(), next.stateData()));
        }
    }

    /**
     * 默认仅以 ERROR 级别记录 {@link FSM.Failure} 停止原因，子类可覆写。
     *
     * @param reason 停止原因
     */
    protected void logTermination(FSM.Reason reason) {
        if (reason instanceof FSM.Failure(Object cause)) {
            io.github.espen7.tapio.event.LoggingAdapter adapter = io.github.espen7.tapio.event.Logging.getLogger(
                    getContext().system(), getSelf().path().toString(), getClass());
            if (cause instanceof Throwable throwable) {
                adapter.error(throwable, "terminating due to Failure");
            } else {
                adapter.error(String.valueOf(cause));
            }
        }
    }

    @Override
    public void postStop() throws Exception {
        try {
            timers.values().forEach(NamedTimer::cancel);
            timers.clear();
            if (currentState != null && currentState.stopReason() == null) {
                terminate(currentState.withStopReason(FSM.Shutdown.INSTANCE));
            }
        } finally {
            super.postStop();
        }
    }
}
