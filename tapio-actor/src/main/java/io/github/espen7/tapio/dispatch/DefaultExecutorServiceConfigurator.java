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
package io.github.espen7.tapio.dispatch;

import com.typesafe.config.Config;
import io.github.espen7.tapio.event.Debug;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** 将 default-executor 委托给外部 ExecutionContext 或配置的 fallback executor。 */
public final class DefaultExecutorServiceConfigurator extends ExecutorServiceConfigurator {
    private final ExecutorServiceFactoryProvider provider;

    /**
     * 创建 default-executor configurator。
     *
     * @param config default-executor 配置
     * @param prerequisites Dispatcher 运行时组件
     * @param fallback fallback executor configurator
     */
    public DefaultExecutorServiceConfigurator(Config config, DispatcherPrerequisites prerequisites,
                                              ExecutorServiceConfigurator fallback) {
        super(config, prerequisites);
        Optional<ExecutionContext> defaultExecutionContext = prerequisites.defaultExecutionContext();
        if (defaultExecutionContext.isPresent()) {
            prerequisites.eventStream().publish(new Debug(
                    "DefaultExecutorServiceConfigurator", getClass(),
                    "Using passed in ExecutionContext as default executor for this ActorSystem. "
                            + "If you want to use a different executor, please specify one in "
                            + io.github.espen7.tapio.actor.ActorPath.Scheme
                            + ".actor.default-dispatcher.default-executor."));
            this.provider = new ExecutionContextExecutorService(defaultExecutionContext.get());
        } else {
            this.provider = fallback;
        }
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        return provider.createExecutorServiceFactory(id, threadFactory);
    }

    private static final class ExecutionContextExecutorService extends AbstractExecutorService
            implements ExecutorServiceFactory, ExecutorServiceFactoryProvider {
        private final ExecutionContext executionContext;

        private ExecutionContextExecutorService(ExecutionContext executionContext) {
            this.executionContext = executionContext;
        }

        @Override
        public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
            return this;
        }

        @Override
        public ExecutorService createExecutorService() {
            return this;
        }

        @Override
        public void execute(Runnable command) {
            executionContext.execute(command);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }
}
