/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.event.LogEvent;

import java.io.Serializable;

/**
 * 远程传输层生命周期事件基接口。
 */
public sealed interface RemotingLifecycleEvent extends Serializable
        permits AssociationEvent, RemotingListenEvent, RemotingShutdownEvent, RemotingErrorEvent,
        QuarantinedEvent, GracefulShutdownQuarantinedEvent, ThisActorSystemQuarantinedEvent {

    /**
     * 返回该事件对应的建议日志级别。
     *
     * @return 日志级别
     */
    LogEvent.Level logLevel();
}
