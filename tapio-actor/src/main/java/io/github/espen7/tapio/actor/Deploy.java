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
 * 按 ActorPath 描述部署配置。
 */
public record Deploy(String path, Scope scope, String dispatcher, String mailbox) implements Serializable {
    /** 未显式指定 dispatcher 时的哨兵值。 */
    public static final String NoDispatcherGiven = "";
    /** 未显式指定 mailbox 时的哨兵值。 */
    public static final String NoMailboxGiven = "";
    /** 要求使用 parent 的 dispatcher 的哨兵值。 */
    public static final String DispatcherSameAsParent = "..";

    /** 使用空 dispatcher、mailbox 与本地作用域的部署。 */
    public static final Deploy local = new Deploy("", LocalScope.INSTANCE, NoDispatcherGiven, NoMailboxGiven);

    /**
     * 创建部署配置。
     *
     * @param path 部署配置使用的 ActorPath
     * @param scope 部署作用域；为 null 时回退为 {@link NoScopeGiven#INSTANCE}
     * @param dispatcher dispatcher id；未指定时为 {@link #NoDispatcherGiven}
     * @param mailbox mailbox id；未指定时为 {@link #NoMailboxGiven}
     */
    public Deploy {
        if (path == null) path = "";
        if (scope == null) scope = NoScopeGiven.INSTANCE;
        if (dispatcher == null) dispatcher = NoDispatcherGiven;
        if (mailbox == null) mailbox = NoMailboxGiven;
    }

    /**
     * 使用默认作用域创建部署配置。
     *
     * @param path 部署配置使用的 ActorPath
     * @param dispatcher dispatcher id
     * @param mailbox mailbox id
     */
    public Deploy(String path, String dispatcher, String mailbox) {
        this(path, NoScopeGiven.INSTANCE, dispatcher, mailbox);
    }

    /**
     * 使用指定作用域创建部署配置。
     *
     * @param scope 部署作用域
     */
    public Deploy(Scope scope) {
        this("", scope, NoDispatcherGiven, NoMailboxGiven);
    }

    /**
     * 合并两个部署配置，{@code this} 的值优先。
     *
     * @param other 回退使用的部署配置
     * @return 合并后的新部署配置
     */
    public Deploy withFallback(Deploy other) {
        return new Deploy(
                path,
                scope.withFallback(other.scope),
                NoDispatcherGiven.equals(dispatcher) ? other.dispatcher : dispatcher,
                NoMailboxGiven.equals(mailbox) ? other.mailbox : mailbox);
    }
}
