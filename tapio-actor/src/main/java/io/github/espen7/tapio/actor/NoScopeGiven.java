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
 * 未指定部署作用域时的默认回退哨兵。
 */
public final class NoScopeGiven extends LocalScope {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 单例实例。 */
    public static final NoScopeGiven INSTANCE = new NoScopeGiven();

    private NoScopeGiven() {
    }

    /**
     * Java API：获取未指定作用域单例实例。
     *
     * @return {@link #INSTANCE}
     */
    public static NoScopeGiven getInstance() {
        return INSTANCE;
    }

    @Override
    public Scope withFallback(Scope other) {
        return other;
    }

    @Serial
    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public int hashCode() {
        return NoScopeGiven.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoScopeGiven;
    }

    @Override
    public String toString() {
        return "NoScopeGiven";
    }
}
