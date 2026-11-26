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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Actor 名称使用的 Pekko 编码。 */
public final class Helpers {
    private static final String base64chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+~";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS");
    private static final ZoneId timeZone = ZoneId.systemDefault();

    private Helpers() {
    }

    /**
     * 将毫秒时间戳格式化为一天内的 UTC 时间。
     *
     * @param timestamp 毫秒时间戳
     * @return {@code HH:mm:ss.SSSUTC} 格式文本
     */
    public static String currentTimeMillisToUTCString(long timestamp) {
        long timeOfDay = timestamp % 86400000L;
        long hours = timeOfDay / 3600000L;
        long minutes = timeOfDay / 60000L % 60;
        long seconds = timeOfDay / 1000L % 60;
        long ms = timeOfDay % 1000;
        StringBuilder sb = new StringBuilder(15);
        if (hours < 10) sb.append(0);
        sb.append(hours).append(':');
        if (minutes < 10) sb.append(0);
        sb.append(minutes).append(':');
        if (seconds < 10) sb.append(0);
        sb.append(seconds).append('.');
        if (ms < 100) sb.append(0);
        if (ms < 10) sb.append(0);
        return sb.append(ms).append("UTC").toString();
    }

    /**
     * 按系统默认时区格式化日志时间戳。
     *
     * @param time 毫秒时间戳
     * @return {@code MM/dd/yyyy HH:mm:ss.SSS} 格式文本
     */
    public static String timestamp(long time) {
        return formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), timeZone));
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
