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

import io.github.espen7.tapio.actor.dungeon.TimerSchedulerImpl;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.japi.pf.PartialFunction;

import java.util.Optional;

/**
 * 支持通过 {@link TimerScheduler} 发送生命周期绑定定时消息的 Java Actor 基类，对应 Pekko
 * {@code AbstractActorWithTimers}。
 *
 * <p>所有定时器在 Actor 重启（{@code preRestart}）或停止（{@code postStop}）时自动取消。</p>
 */
public abstract class AbstractActorWithTimers extends AbstractActor {
    private final TimerSchedulerImpl timers = new TimerSchedulerImpl(getContext());

    /** 创建支持定时器的 Actor 实例。 */
    protected AbstractActorWithTimers() {
    }

    /**
     * 返回绑定此 Actor 生命周期的 {@link TimerScheduler}。
     *
     * @return TimerScheduler 实例
     */
    public final TimerScheduler getTimers() {
        return timers;
    }

    @Override
    protected void aroundPreRestart(Throwable reason, Optional<Object> message) throws Exception {
        timers.cancelAll();
        super.aroundPreRestart(reason, message);
    }

    @Override
    protected void aroundPostStop() throws Exception {
        timers.cancelAll();
        super.aroundPostStop();
    }

    @Override
    protected void aroundReceive(PartialFunction<Object, Void> receive, Object msg) throws Exception {
        if (msg instanceof TimerSchedulerImpl.TimerMsg timerMsg) {
            Optional<Object> unwrapped = timers.interceptTimerMsg(timerMsg);
            if (unwrapped.isEmpty()) {
                return;
            }
            Object actual = unwrapped.get();
            ActorCell cell = (ActorCell) getContext();
            if (actual instanceof AutoReceivedMessage) {
                cell.autoReceiveMessage(new Envelope(actual, getSelf()));
            } else {
                Envelope current = cell.currentMessage();
                cell.setCurrentMessage(new Envelope(actual, current == null ? getSelf() : current.sender()));
                super.aroundReceive(receive, actual);
            }
            return;
        }
        super.aroundReceive(receive, msg);
    }
}
