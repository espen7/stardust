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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.Optional;

/**
 * 指数退避监督者 Actor 工厂与协议消息定义，对应 Pekko {@code BackoffSupervisor}。
 */
public final class BackoffSupervisor {
    private BackoffSupervisor() {
    }

    /** 根据 {@link BackoffOnStopOptions} 创建监督者 {@link Props}。 */
    public static Props props(BackoffOnStopOptions options) {
        return options.props();
    }

    /** 根据 {@link BackoffOnFailureOptions} 创建监督者 {@link Props}。 */
    public static Props props(BackoffOnFailureOptions options) {
        return options.props();
    }

    /** 查询当前子 Actor 引用的请求消息。 */
    public static final Object GetCurrentChild = new Object() {
        @Override
        public String toString() {
            return "GetCurrentChild";
        }
    };

    /** Java API：返回 {@link #GetCurrentChild}。 */
    public static Object getCurrentChild() {
        return GetCurrentChild;
    }

    /** {@link #GetCurrentChild} 的回复消息。 */
    public record CurrentChild(Optional<ActorRef> ref) {
        /** Java API：返回当前子 Actor 引用。 */
        public Optional<ActorRef> getRef() {
            return ref;
        }
    }

    /** 手动重置退避计数的消息（需配合 {@code withManualReset()}）。 */
    public static final Object Reset = new Object() {
        @Override
        public String toString() {
            return "Reset";
        }
    };

    /** Java API：返回 {@link #Reset}。 */
    public static Object reset() {
        return Reset;
    }

    /** 查询当前重启次数的请求消息。 */
    public static final Object GetRestartCount = new Object() {
        @Override
        public String toString() {
            return "GetRestartCount";
        }
    };

    /** Java API：返回 {@link #GetRestartCount}。 */
    public static Object getRestartCount() {
        return GetRestartCount;
    }

    /** {@link #GetRestartCount} 的回复消息。 */
    public record RestartCount(int count) {
    }

    /** INTERNAL API：触发启动子 Actor 的内部消息。 */
    @InternalApi
    public static final class StartChild implements DeadLetterSuppression {
        public static final StartChild INSTANCE = new StartChild();

        private StartChild() {
        }
    }

    /** INTERNAL API：在稳定运行时间窗结束后重置退避计数的内部消息。 */
    @InternalApi
    public record ResetRestartCount(int current) implements DeadLetterSuppression {
    }
}
