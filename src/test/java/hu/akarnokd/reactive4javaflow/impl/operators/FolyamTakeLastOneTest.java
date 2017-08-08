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

public class FolyamTakeLastOneTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.just(1).takeLast(1)
        , 1);
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.just(1).takeLast(0)
                );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty().takeLast(1)
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).takeLast(1)
                , 5);
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.range(1, 5).last(),
                5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .takeLast(1)
                .test()
                .assertFailure(IOException.class);
    }
}
