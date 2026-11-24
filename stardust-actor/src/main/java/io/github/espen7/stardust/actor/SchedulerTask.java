package io.github.espen7.stardust.actor;

/**
 * 由 {@link Scheduler} 调度且能接收取消通知的任务。
 */
public interface SchedulerTask extends Runnable {
    /**
     * 在任务成功取消后由调度时提供的 Executor 调用。默认无操作。
     */
    default void cancelled() {
    }
}
