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
package io.github.espen7.tapio.cluster.ddata;

import java.io.Serial;

/**
 * {@link LWWRegister} 的类型化键。
 *
 * @param <A> 寄存器值类型
 */
public final class LWWRegisterKey<A> extends Key<LWWRegister<A>> {
    @Serial
    private static final long serialVersionUID = 1L;

    private LWWRegisterKey(String id) {
        super(id);
    }

    /** 创建 {@link LWWRegisterKey}。 */
    public static <A> LWWRegisterKey<A> create(String id) {
        return new LWWRegisterKey<>(id);
    }
}
