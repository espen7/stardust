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

/**
 * 支持在创建后绑定被观察主机地址的 {@link FailureDetector} 扩展接口。
 */
public interface FailureDetectorWithAddress {

    /**
     * 设置被观察主机的地址描述。
     *
     * @param addr 地址字符串
     */
    void setAddress(String addr);
}
