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
package io.github.espen7.tapio.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * 优先使用给定 {@link ClassLoader} 加载反序列化类的 {@link ObjectInputStream}。
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {
    private final ClassLoader classLoader;

    /**
     * 创建使用指定类加载器的对象输入流。
     *
     * @param classLoader 优先使用的类加载器
     * @param is 底层输入流
     * @throws IOException 读取流头失败时抛出
     */
    public ClassLoaderObjectInputStream(ClassLoader classLoader, InputStream is) throws IOException {
        super(is);
        this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass objectStreamClass) throws IOException, ClassNotFoundException {
        try {
            return Class.forName(objectStreamClass.getName(), false, classLoader);
        } catch (ClassNotFoundException ignored) {
            return super.resolveClass(objectStreamClass);
        }
    }
}
