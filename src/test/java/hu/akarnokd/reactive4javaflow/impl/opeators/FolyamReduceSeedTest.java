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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.io.IOException;

public class FolyamReduceSeedTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.range(1, 5)
            .reduce(() -> 10, (a, b) -> a + b), 25);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.just(1)
                        .reduce(() -> 10, (a, b) -> a + b),
                11
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.<Integer>empty()
                        .reduce(() -> 10, (a, b) -> a + b),
                10
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .reduce(() -> 10, (a, b) -> a)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void initialSupplierThrows() {
        Folyam.range(1, 3)
                .reduce(() -> { throw new IOException(); }, (a, b) -> b)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void reducerThrows() {
        Folyam.range(1, 3)
                .reduce(() -> 10, (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }
}
