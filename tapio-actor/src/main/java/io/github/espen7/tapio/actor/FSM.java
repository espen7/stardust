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

import io.github.espen7.tapio.annotation.DoNotInherit;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 有限状态机（FSM）协议类型与状态容器。
 */
public final class FSM {
    private FSM() {
    }

    /** 状态超时触发时向 FSM Actor 投递的标记消息。 */
    public static final Object StateTimeout = new Object() {
        @Override
        public String toString() {
            return "StateTimeout";
        }
    };

    /** Java API：返回 {@link #StateTimeout} 单例。 */
    public static Object stateTimeout() {
        return StateTimeout;
    }

    /** 包装当前消息与状态数据的事件对象。 */
    public record Event<D>(Object event, D stateData) {
    }

    /** FSM 停止原因。 */
    @DoNotInherit
    public sealed interface Reason permits Normal, Shutdown, Failure {
    }

    /** 正常停止原因。 */
    public static final class Normal implements Reason {
        public static final Normal INSTANCE = new Normal();

        private Normal() {
        }
    }

    /** 收到外部关闭指令的停止原因。 */
    public static final class Shutdown implements Reason {
        public static final Shutdown INSTANCE = new Shutdown();

        private Shutdown() {
        }
    }

    /** 异常或失败导致的停止原因。 */
    public record Failure(Object cause) implements Reason {
    }

    /** Java API：返回 {@link Normal#INSTANCE}。 */
    public static Reason normal() {
        return Normal.INSTANCE;
    }

    /** Java API：返回 {@link Shutdown#INSTANCE}。 */
    public static Reason shutdown() {
        return Shutdown.INSTANCE;
    }

    /** FSM 终止回调接收的事件对象。 */
    public record StopEvent<S, D>(Reason reason, S currentState, D stateData) {
    }

    /**
     * 描述一次状态转换目标、数据、超时与待发送回复的状态对象。
     *
     * @param <S> 状态类型
     * @param <D> 状态数据类型
     */
    public static final class State<S, D> {
        private final S stateName;
        private final D stateData;
        private final Duration timeout;
        private final Reason stopReason;
        private final List<Object> replies;
        private final boolean notifying;

        State(S stateName, D stateData, Duration timeout, Reason stopReason, List<Object> replies, boolean notifying) {
            this.stateName = stateName;
            this.stateData = stateData;
            this.timeout = timeout;
            this.stopReason = stopReason;
            this.replies = replies;
            this.notifying = notifying;
        }

        /** 返回目标状态名。 */
        public S stateName() {
            return stateName;
        }

        /** 返回目标状态数据。 */
        public D stateData() {
            return stateData;
        }

        /** 返回状态超时时间（可为 {@code null}）。 */
        public Duration timeout() {
            return timeout;
        }

        /** 返回停止原因（未停止时为 {@code null}）。 */
        public Reason stopReason() {
            return stopReason;
        }

        /** 返回本次转换附带的回复消息列表。 */
        public List<Object> replies() {
            return replies;
        }

        boolean notifying() {
            return notifying;
        }

        /** 指定转换后的新状态数据。 */
        public State<S, D> using(D nextStateData) {
            return new State<>(stateName, nextStateData, timeout, stopReason, replies, notifying);
        }

        /** 指定本状态的超时时长（{@code null} 表示取消默认超时）。 */
        public State<S, D> forMax(Duration duration) {
            return new State<>(stateName, stateData, duration, stopReason, replies, notifying);
        }

        /** 追加一条向当前 sender 发送的回复消息。 */
        public State<S, D> replying(Object replyValue) {
            List<Object> next = new ArrayList<>(replies.size() + 1);
            next.add(replyValue);
            next.addAll(replies);
            return new State<>(stateName, stateData, timeout, stopReason, List.copyOf(next), notifying);
        }

        State<S, D> withStopReason(Reason reason) {
            return new State<>(stateName, stateData, timeout, reason, replies, notifying);
        }

        State<S, D> withNotifying(boolean value) {
            return new State<>(stateName, stateData, timeout, stopReason, replies, value);
        }
    }
}
