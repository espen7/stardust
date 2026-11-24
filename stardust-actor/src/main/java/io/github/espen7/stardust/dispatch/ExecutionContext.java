package io.github.espen7.stardust.dispatch;

import java.util.concurrent.Executor;

/**
 * 能报告任务提交失败的执行上下文，对应 Scala 的 ExecutionContext。
 */
public interface ExecutionContext extends Executor {
    /**
     * 报告执行或提交任务时发生的非致命异常。
     *
     * @param cause 需要报告的异常
     */
    void reportFailure(Throwable cause);
}
