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

import java.io.Serializable;

/**
 * DeathWatch 发送给 watcher 的终止通知消息。
 */
public final class Terminated implements Serializable {
    private final ActorRef actor;
    private final boolean existenceConfirmed;
    private final boolean addressTerminated;

    public Terminated(ActorRef actor) {
        this(actor, true, false);
    }

    public Terminated(ActorRef actor, boolean existenceConfirmed, boolean addressTerminated) {
        this.actor = actor;
        this.existenceConfirmed = existenceConfirmed;
        this.addressTerminated = addressTerminated;
    }

    public ActorRef actor() {
        return actor;
    }

    public ActorRef getActor() {
        return actor;
    }

    public boolean existenceConfirmed() {
        return existenceConfirmed;
    }

    public boolean addressTerminated() {
        return addressTerminated;
    }

    @Override
    public String toString() {
        return "Terminated(" + actor + ")";
    }
}
