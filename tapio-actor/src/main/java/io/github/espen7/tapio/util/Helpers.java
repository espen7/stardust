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
package io.github.espen7.tapio.util;

/** Actor 名称使用的 Pekko 编码。 */
public final class Helpers {
    private static final String base64chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+~";

    private Helpers() {
    }

    /**
     * 按从低位到高位的顺序，每六位向指定 builder 追加一个字符。
     *
     * @param number 待编码的计数值
     * @param builder 已包含名称前缀的 builder
     * @return 追加编码后的名称
     */
    public static String base64(long number, StringBuilder builder) {
        do {
            builder.append(base64chars.charAt((int) number & 63));
            number >>>= 6;
        } while (number != 0);
        return builder.toString();
    }
}
