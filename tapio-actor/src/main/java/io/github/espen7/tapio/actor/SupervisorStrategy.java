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

import java.util.Collection;
import java.util.function.Function;
import io.github.espen7.tapio.actor.dungeon.ChildRestartStats;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.Warning;

import java.lang.reflect.InvocationTargetException;

/**
 * 将 child failure 映射为监督指令并执行相应的生命周期操作。
 *
 * <p>Resume 保留当前实例，Restart 重建实例，Stop 终止 child，Escalate 将失败
 * 交给更上层 supervisor。defaultDecider 对普通 Exception 执行 Restart。</p>
 */
public abstract class SupervisorStrategy {
    /**
     * 监督器对 child failure 作出的决定。
     */
    public enum Directive {Resume, Restart, Stop, Escalate}

    /**
     * 默认决策：初始化失败、ActorKilledException 和 DeathPactException 停止，普通异常重启。
     */
    public static final Function<Throwable, Directive> defaultDecider = error -> {
        if (error instanceof ActorInitializationException || error instanceof ActorKilledException
                || error instanceof DeathPactException) return Directive.Stop;
        if (error instanceof Exception) return Directive.Restart;
        return Directive.Escalate;
    };
    public static final SupervisorStrategy defaultStrategy = new OneForOneStrategy(-1, java.time.Duration.ofNanos(-1), defaultDecider);
    public static final SupervisorStrategy stoppingStrategy = new OneForOneStrategy(-1, java.time.Duration.ofNanos(-1),
            error -> error instanceof Exception ? Directive.Stop : Directive.Escalate);

    /**
     * 返回此策略的决策函数。
     *
     * @return 将异常映射为监督指令的函数
     */
    public Function<Throwable, Directive> decider() {
        return this::handle;
    }

    /**
     * 根据失败原因计算监督指令。
     *
     * @param cause child failure 原因
     * @return 监督指令
     */
    public abstract Directive handle(Throwable cause);

    /**
     * 返回失败日志是否启用。
     *
     * @return 启用时为 {@code true}
     */
    public boolean loggingEnabled() {
        return true;
    }

    /**
     * 对失败 child 执行 restart 或 stop 操作。
     *
     * @param context 当前 supervisor 的 ActorContext
     * @param restart 为 {@code true} 时 restart child，否则停止 child
     * @param child 失败的 child
     * @param cause 失败原因
     * @param stats 失败 child 的重启统计
     * @param children 当前所有 child 的重启统计
     */
    public void processFailure(ActorContext context, boolean restart, ActorRef child, Throwable cause,
                               ChildRestartStats stats, Collection<ChildRestartStats> children) {
        if (restart) restartChild(child, cause, false);
        else context.stop(child);
    }

    /**
     * 处理 child failure 并返回该失败是否已处理。
     *
     * @param context 当前 supervisor 的 ActorContext
     * @param child 失败的 child
     * @param cause 失败原因
     * @param stats 失败 child 的重启统计
     * @param children 当前所有 child 的重启统计
     * @return 指令已执行时为 {@code true}，需要向上升级时为 {@code false}
     */
    public boolean handleFailure(ActorContext context, ActorRef child, Throwable cause, ChildRestartStats stats,
                                 Collection<ChildRestartStats> children) {
        Directive directive = decider().apply(cause);
        if (directive == null) directive = Directive.Escalate;
        switch (directive) {
            case Resume -> {
                logFailure(context, child, cause, directive);
                resumeChild(child, cause);
                return true;
            }
            case Restart -> {
                logFailure(context, child, cause, directive);
                processFailure(context, true, child, cause, stats, children);
                return true;
            }
            case Stop -> {
                logFailure(context, child, cause, directive);
                processFailure(context, false, child, cause, stats, children);
                return true;
            }
            case Escalate -> {
                logFailure(context, child, cause, directive);
                return false;
            }
        }
        throw new IllegalStateException("Unknown directive: " + directive);
    }

    /**
     * 在 child 终止并从 parent children 集合移除后调用。
     *
     * @param context 当前 supervisor 的 ActorContext
     * @param child 已终止的 child
     * @param children 剩余 child 引用
     */
    public void handleChildTerminated(ActorContext context, ActorRef child, Collection<ActorRef> children) {
    }

    /**
     * 按监督指令级别向 EventStream 发布失败日志。
     *
     * @param context 当前 supervisor 的 ActorContext
     * @param child 失败的 child
     * @param cause 失败原因
     * @param decision 监督指令
     */
    public void logFailure(ActorContext context, ActorRef child, Throwable cause, Directive decision) {
        if (!loggingEnabled() || decision == Directive.Escalate) return;
        Throwable messageCause = cause;
        if (cause instanceof ActorInitializationException && cause.getCause() != null) {
            Throwable initializationCause = cause.getCause();
            messageCause = initializationCause instanceof InvocationTargetException invocation
                    && invocation.getCause() != null ? invocation.getCause() : initializationCause;
        }
        String message = String.valueOf(messageCause.getMessage());
        try {
            if (decision == Directive.Resume) {
                context.system().eventStream().publish(new Warning(child.path().toString(), getClass(), message, cause));
            } else {
                context.system().eventStream().publish(new Error(child.path().toString(), getClass(), message, cause));
            }
        } catch (RuntimeException ignored) {
        }
    }

    /**
     * 恢复 child 的消息处理。
     *
     * @param child 要恢复的 child
     * @param cause 引发恢复的失败原因
     */
    protected final void resumeChild(ActorRef child, Throwable cause) {
        if (child instanceof InternalActorRef internalChild) {
            internalChild.sendSystemMessage(new SystemMessage.Resume(cause));
        }
    }

    /**
     * 重建 child，可在发送 Recreate 前先暂停它。
     *
     * @param child 要重建的 child
     * @param cause 引发重建的失败原因
     * @param suspendFirst 是否先发送 Suspend
     */
    protected final void restartChild(ActorRef child, Throwable cause, boolean suspendFirst) {
        if (child instanceof InternalActorRef internalChild) {
            if (suspendFirst) internalChild.sendSystemMessage(new SystemMessage.Suspend());
            internalChild.sendSystemMessage(new SystemMessage.Recreate(cause));
        }
    }

    /**
     * 将重试时间窗转换为毫秒数。
     *
     * @param withinTimeRange 重试时间窗
     * @return 毫秒数；负 duration 返回 {@code null} 表示无时间窗
     */
    protected static Integer windowMillis(java.time.Duration withinTimeRange) {
        if (withinTimeRange.isNegative()) return null;
        try {
            return (int) Math.min(Integer.MAX_VALUE, withinTimeRange.toMillis());
        } catch (ArithmeticException ignored) {
            return Integer.MAX_VALUE;
        }
    }
}
