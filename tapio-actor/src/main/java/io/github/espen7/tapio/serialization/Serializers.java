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
package io.github.espen7.tapio.serialization;

/**
 * 序列化辅助工具方法。
 */
public final class Serializers {
    private Serializers() {
    }

    /**
     * 为给定序列化器和消息计算线协议 manifest 字符串。
     *
     * @param s 使用的序列化器
     * @param message 待序列化消息
     * @return manifest 字符串；不需要时返回空字符串 {@code ""}
     */
    public static String manifestFor(Serializer s, Object message) {
        if (s instanceof SerializerWithStringManifest s2) {
            return s2.manifest(message);
        }
        return s.includeManifest() ? message.getClass().getName() : "";
    }
}
