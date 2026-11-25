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

/** 用于 ActorSelection 的通配符匹配。 */
public final class Glob {
    private Glob() {
    }

    /**
     * 判断整个字符串是否匹配模式。
     *
     * @param pattern 模式，问号匹配单字符，星号匹配任意长度
     * @param input 待匹配的字符串
     * @return 字符串是否匹配
     */
    public static boolean matches(String pattern, String input) {
        int p = 0;
        int i = 0;
        int starP = -1;
        int starI = -1;
        while (i < input.length()) {
            if (p < pattern.length() && (pattern.charAt(p) == '?' || pattern.charAt(p) == input.charAt(i))) {
                p++;
                i++;
            } else if (p < pattern.length() && pattern.charAt(p) == '*') {
                starP = p++;
                starI = i;
            } else if (starP >= 0) {
                i = ++starI;
                p = starP + 1;
            } else {
                return false;
            }
        }
        while (p < pattern.length() && pattern.charAt(p) == '*') p++;
        return p == pattern.length();
    }
}
