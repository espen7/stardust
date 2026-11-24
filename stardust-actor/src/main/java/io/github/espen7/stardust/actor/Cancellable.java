package io.github.espen7.stardust.actor;

/**
 * 一次性调度任务的取消句柄。
 */
public interface Cancellable {

    boolean cancel();

    boolean isCancelled();
}
