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

import java.util.concurrent.TimeUnit;

/**
 * 保存一个 child incarnation 的重启统计。
 *
 * <p>统计状态归属于当前 ActorRef。child 以新 UID 重新创建时会获得新的统计对象。</p>
 */
public final class ChildRestartStats implements ChildStats {
    private final ActorRef child;
    private int maxNrOfRetriesCount;
    private long restartTimeWindowStartNanos;

    /**
     * 创建计数为零且尚未启动时间窗的统计对象。
     *
     * @param child 被监督的 child 引用
     */
    public ChildRestartStats(ActorRef child) {
        this(child, 0, 0L);
    }

    /**
     * 使用指定的重试状态创建统计对象。
     *
     * @param child 被监督的 child 引用
     * @param maxNrOfRetriesCount 已使用的重试次数
     * @param restartTimeWindowStartNanos 时间窗起点的单调时钟纳秒值
     */
    public ChildRestartStats(ActorRef child, int maxNrOfRetriesCount, long restartTimeWindowStartNanos) {
        this.child = child;
        this.maxNrOfRetriesCount = maxNrOfRetriesCount;
        this.restartTimeWindowStartNanos = restartTimeWindowStartNanos;
    }

    /**
     * 返回被监督的 child。
     *
     * @return child 引用
     */
    public ActorRef child() {
        return child;
    }

    /**
     * 返回当前重试计数。
     *
     * @return 已使用的重试次数
     */
    public int maxNrOfRetriesCount() {
        return maxNrOfRetriesCount;
    }

    /**
     * 更新当前重试计数。
     *
     * @param maxNrOfRetriesCount 新的重试计数
     */
    public void maxNrOfRetriesCount(int maxNrOfRetriesCount) {
        this.maxNrOfRetriesCount = maxNrOfRetriesCount;
    }

    /**
     * 返回重试时间窗的单调时钟起点。
     *
     * @return 起点纳秒值，零表示尚未启动时间窗
     */
    public long restartTimeWindowStartNanos() {
        return restartTimeWindowStartNanos;
    }

    /**
     * 更新重试时间窗的单调时钟起点。
     *
     * @param restartTimeWindowStartNanos 新的起点纳秒值
     */
    public void restartTimeWindowStartNanos(long restartTimeWindowStartNanos) {
        this.restartTimeWindowStartNanos = restartTimeWindowStartNanos;
    }

    /**
     * 返回 child incarnation UID。
     *
     * @return child ActorPath 中的 UID
     */
    public int uid() {
        return child.path().uid();
    }

    /**
     * 检查当前重启是否仍在策略限制内，并按 Pekko 规则更新统计状态。
     *
     * @param retries 最大重试次数，负数表示不限制次数
     * @param windowMillis 时间窗毫秒数，{@code null} 表示没有时间窗
     * @return 允许重启时为 {@code true}
     */
    public boolean requestRestartPermission(int retries, Integer windowMillis) {
        if (retries >= 0 && retries < 1) return false;
        if (retries >= 0 && windowMillis == null) {
            maxNrOfRetriesCount++;
            return maxNrOfRetriesCount <= retries;
        }
        if (windowMillis != null) return retriesInWindowOkay(retries >= 0 ? retries : 1, windowMillis);
        return true;
    }

    private boolean retriesInWindowOkay(int retries, int windowMillis) {
        int retriesDone = maxNrOfRetriesCount + 1;
        long now = System.nanoTime();
        long windowStart = restartTimeWindowStartNanos;
        if (windowStart == 0) {
            restartTimeWindowStartNanos = now;
            windowStart = now;
        }
        boolean insideWindow = now - windowStart <= TimeUnit.MILLISECONDS.toNanos(windowMillis);
        if (insideWindow) {
            maxNrOfRetriesCount = retriesDone;
            return retriesDone <= retries;
        }
        maxNrOfRetriesCount = 1;
        restartTimeWindowStartNanos = now;
        return true;
    }
}
