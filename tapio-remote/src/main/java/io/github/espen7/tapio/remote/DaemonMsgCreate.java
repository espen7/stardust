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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serial;
import java.io.Serializable;

/**
 * INTERNAL API：用于在远程节点上创建子 Actor 的守护消息。
 *
 * @param props Actor 创建配置
 * @param deploy 部署配置
 * @param path 目标 Actor 路径
 * @param supervisor 监督者引用
 */
@InternalApi
public record DaemonMsgCreate(Props props, Deploy deploy, String path, ActorRef supervisor) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
