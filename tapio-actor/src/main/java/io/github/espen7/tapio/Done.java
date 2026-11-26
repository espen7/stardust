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
package io.github.espen7.tapio;

import java.io.Serializable;

/** CoordinatedShutdown 成功完成时的结果。 */
public final class Done implements Serializable {
    private static final Done INSTANCE = new Done();

    private Done() {
    }

    /**
     * 返回 Done singleton。
     *
     * @return Done singleton
     */
    public static Done getInstance() {
        return INSTANCE;
    }

    /**
     * 返回 Done singleton。
     *
     * @return Done singleton
     */
    public static Done done() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "Done";
    }
}
