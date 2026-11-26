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
package io.github.espen7.tapio.event;

import org.slf4j.Marker;

import java.util.Map;
import java.util.Objects;

/** 保留原始 SLF4J Marker 及其引用关系的日志 marker。 */
public final class Slf4jLogMarker extends LogMarker {
    private final Marker marker;

    /**
     * 包装原始 SLF4J Marker。
     *
     * @param marker 原始 SLF4J Marker
     */
    public Slf4jLogMarker(Marker marker) {
        super(Objects.requireNonNull(marker, "marker").getName(), Map.of());
        this.marker = marker;
    }

    /**
     * 返回原始 SLF4J Marker。
     *
     * @return 原始 Marker
     */
    public Marker marker() {
        return marker;
    }

    /**
     * Java API 的 SLF4J Marker 包装工厂。
     *
     * @param marker 原始 SLF4J Marker
     * @return 包装后的日志 marker
     */
    public static Slf4jLogMarker create(Marker marker) {
        return new Slf4jLogMarker(marker);
    }
}
