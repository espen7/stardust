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

import io.github.espen7.tapio.actor.ActorContext;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;
import io.github.espen7.tapio.actor.NotInfluenceReceiveTimeout;
import io.github.espen7.tapio.actor.TimerScheduler;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@link TimerScheduler} 的内部实现，对应 Pekko {@code TimerSchedulerImpl}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public final class TimerSchedulerImpl extends TimerScheduler {
    /** 定时器内部包装消息标记接口。 */
    public interface TimerMsg {
        Object key();

        int generation();

        TimerSchedulerImpl owner();
    }

    private record Timer(Object key, Object msg, boolean repeat, int generation, Cancellable task) {
    }

    /** 影响 ReceiveTimeout 的定时器内部消息。 */
    public record InfluenceReceiveTimeoutTimerMsg(Object key, int generation, TimerSchedulerImpl owner)
            implements TimerMsg, NoSerializationVerificationNeeded {
    }

    /** 不影响 ReceiveTimeout 的定时器内部消息。 */
    public record NotInfluenceReceiveTimeoutTimerMsg(Object key, int generation, TimerSchedulerImpl owner)
            implements TimerMsg, NoSerializationVerificationNeeded, NotInfluenceReceiveTimeout {
    }

    private sealed interface TimerMode permits FixedRateMode, FixedDelayMode, SingleMode {
        boolean repeat();
    }

    private record FixedRateMode(Duration initialDelay) implements TimerMode {
        @Override
        public boolean repeat() {
            return true;
        }
    }

    private record FixedDelayMode(Duration initialDelay) implements TimerMode {
        @Override
        public boolean repeat() {
            return true;
        }
    }

    private static final class SingleMode implements TimerMode {
        private static final SingleMode INSTANCE = new SingleMode();

        @Override
        public boolean repeat() {
            return false;
        }
    }

    private final ActorContext ctx;
    private final LoggingAdapter log;
    private final Map<Object, Timer> timers = new HashMap<>();
    private int timerGen = 0;

    /**
     * 创建绑定指定 ActorContext 的定时器调度器。
     *
     * @param ctx 所属 ActorContext
     */
    public TimerSchedulerImpl(ActorContext ctx) {
        this.ctx = ctx;
        this.log = Logging.getLogger(ctx.system().eventStream(), ctx.self().path().toString(), TimerScheduler.class);
    }

    private int nextTimerGen() {
        timerGen += 1;
        return timerGen;
    }

    @Override
    public void startTimerAtFixedRate(Object key, Object msg, Duration interval) {
        startTimer(key, msg, interval, new FixedRateMode(interval));
    }

    @Override
    public void startTimerAtFixedRate(Object key, Object msg, Duration initialDelay, Duration interval) {
        startTimer(key, msg, interval, new FixedRateMode(initialDelay));
    }

    @Override
    public void startTimerWithFixedDelay(Object key, Object msg, Duration delay) {
        startTimer(key, msg, delay, new FixedDelayMode(delay));
    }

    @Override
    public void startTimerWithFixedDelay(Object key, Object msg, Duration initialDelay, Duration delay) {
        startTimer(key, msg, delay, new FixedDelayMode(initialDelay));
    }

    @Override
    public void startSingleTimer(Object key, Object msg, Duration timeout) {
        startTimer(key, msg, timeout, SingleMode.INSTANCE);
    }

    private void startTimer(Object key, Object msg, Duration timeout, TimerMode mode) {
        Timer existing = timers.get(key);
        if (existing != null) {
            cancelTimer(existing);
        }
        int nextGen = nextTimerGen();
        TimerMsg timerMsg = msg instanceof NotInfluenceReceiveTimeout
                ? new NotInfluenceReceiveTimeoutTimerMsg(key, nextGen, this)
                : new InfluenceReceiveTimeoutTimerMsg(key, nextGen, this);

        Cancellable task = switch (mode) {
            case SingleMode ignored ->
                    ctx.system().scheduler().scheduleOnce(timeout, ctx.self(), timerMsg, ctx.dispatcher(), ctx.self());
            case FixedDelayMode fixedDelay ->
                    ctx.system().scheduler().scheduleWithFixedDelay(
                            fixedDelay.initialDelay(), timeout, ctx.self(), timerMsg, ctx.dispatcher(), ctx.self());
            case FixedRateMode fixedRate ->
                    ctx.system().scheduler().scheduleAtFixedRate(
                            fixedRate.initialDelay(), timeout, ctx.self(), timerMsg, ctx.dispatcher(), ctx.self());
        };
        timers.put(key, new Timer(key, msg, mode.repeat(), nextGen, task));
    }

    @Override
    public boolean isTimerActive(Object key) {
        return timers.containsKey(key);
    }

    @Override
    public void cancel(Object key) {
        Timer existing = timers.get(key);
        if (existing != null) {
            cancelTimer(existing);
        }
    }

    private void cancelTimer(Timer timer) {
        timer.task().cancel();
        timers.remove(timer.key());
    }

    @Override
    public void cancelAll() {
        for (Timer timer : timers.values()) {
            timer.task().cancel();
        }
        timers.clear();
    }

    /**
     * 校验并解包定时器内部消息；若属于已取消、已覆盖或旧重启实例的过期消息，返回 {@link Optional#empty()}。
     *
     * @param timerMsg 收到的内部定时器消息
     * @return 有效时返回原始用户消息
     */
    public Optional<Object> interceptTimerMsg(TimerMsg timerMsg) {
        Timer timer = timers.get(timerMsg.key());
        if (timer == null) {
            log.debug("Received timer [{}] that has been removed, discarding", timerMsg.key());
            return Optional.empty();
        }
        if (timerMsg.owner() != this) {
            log.debug("Received timer [{}] from old restarted instance, discarding", timerMsg.key());
            return Optional.empty();
        }
        if (timerMsg.generation() == timer.generation()) {
            if (!timer.repeat()) {
                timers.remove(timer.key());
            }
            return Optional.ofNullable(timer.msg());
        }
        log.debug("Received timer [{}] from old generation [{}], expected generation [{}], discarding",
                timerMsg.key(), timerMsg.generation(), timer.generation());
        return Optional.empty();
    }
}
