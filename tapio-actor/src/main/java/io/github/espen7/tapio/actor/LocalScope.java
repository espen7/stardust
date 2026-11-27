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
package io.github.espen7.tapio.actor;

import java.io.Serial;

/**
 * 本地部署作用域。未设置其他作用域时的默认作用域，也是 {@link LocalActorRefProvider} 唯一处理的作用域。
 */
public class LocalScope implements Scope {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 单例实例。 */
    public static final LocalScope INSTANCE = new LocalScope();

    /** 构造本地作用域实例。 */
    protected LocalScope() {
    }

    /**
     * Java API：获取本地作用域单例实例。
     *
     * @return {@link #INSTANCE}
     */
    public static LocalScope getInstance() {
        return INSTANCE;
    }

    @Override
    public Scope withFallback(Scope other) {
        return this;
    }

    @Serial
    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public int hashCode() {
        return LocalScope.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LocalScope;
    }

    @Override
    public String toString() {
        return "LocalScope";
    }
}
