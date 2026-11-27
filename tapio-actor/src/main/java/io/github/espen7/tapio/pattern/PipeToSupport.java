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
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Status;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * 将异步 {@link CompletionStage} 的完成结果转发给目标 Actor 或 ActorSelection。
 */
public final class PipeToSupport {
    private PipeToSupport() {
    }

    /**
     * 绑定待转发 {@link CompletionStage} 与执行器的流式构建器。
     *
     * @param <T> 异步结果类型
     */
    public static final class PipeableCompletionStage<T> {
        private final CompletionStage<T> future;
        private final Executor executor;

        PipeableCompletionStage(CompletionStage<T> future, Executor executor) {
            this.future = Objects.requireNonNull(future, "future must not be null");
            this.executor = executor;
        }

        /**
         * 将结果发送到目标 {@link ActorRef}，使用 {@link ActorRef#noSender} 作为发送者。
         *
         * @param recipient 目标 ActorRef
         * @return 当前构建器
         */
        public PipeableCompletionStage<T> to(ActorRef recipient) {
            return to(recipient, ActorRef.noSender);
        }

        /**
         * 将结果发送到目标 {@link ActorRef}。
         *
         * @param recipient 目标 ActorRef
         * @param sender 发送者引用
         * @return 当前构建器
         */
        public PipeableCompletionStage<T> to(ActorRef recipient, ActorRef sender) {
            future.whenComplete((value, failure) -> deliver(recipient, sender, value, failure));
            return this;
        }

        /**
         * 将结果发送到目标 {@link ActorSelection}，使用 {@link ActorRef#noSender} 作为发送者。
         *
         * @param recipient 目标 ActorSelection
         * @return 当前构建器
         */
        public PipeableCompletionStage<T> to(ActorSelection recipient) {
            return to(recipient, ActorRef.noSender);
        }

        /**
         * 将结果发送到目标 {@link ActorSelection}。
         *
         * @param recipient 目标 ActorSelection
         * @param sender 发送者引用
         * @return 当前构建器
         */
        public PipeableCompletionStage<T> to(ActorSelection recipient, ActorRef sender) {
            future.whenComplete((value, failure) -> deliver(recipient, sender, value, failure));
            return this;
        }

        /**
         * 返回底层 {@link CompletionStage}。
         *
         * @return CompletionStage
         */
        public CompletionStage<T> future() {
            return future;
        }

        private static <T> void deliver(ActorRef recipient, ActorRef sender, T value, Throwable failure) {
            if (value != null) {
                recipient.tell(value, sender);
            }
            if (failure != null) {
                recipient.tell(new Status.Failure(failure), sender);
            }
        }

        private static <T> void deliver(ActorSelection recipient, ActorRef sender, T value, Throwable failure) {
            if (value != null) {
                recipient.tell(value, sender);
            }
            if (failure != null) {
                recipient.tell(new Status.Failure(failure), sender);
            }
        }
    }
}
