/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.io.IOException;

public class EsetlegMapTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .map(v -> v + 1),
                2);
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .map(v -> v + 1)
                        .filter(v -> true),
                2);
    }

    @Test
    public void mapperThrows() {
        Esetleg.just(1)
                .map(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperThrowsConditional() {
        Esetleg.just(1)
                .map(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .map(Object::toString)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Esetleg.error(new IOException())
                .map(Object::toString)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
