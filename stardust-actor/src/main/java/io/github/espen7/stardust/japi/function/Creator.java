package io.github.espen7.stardust.japi.function;

/**
 * Actor 实例的延迟创建函数，对应 Pekko japi.function.Creator。
 */
@FunctionalInterface
public interface Creator<T> {
    T create() throws Exception;
}
