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

public class FolyamTakeLastTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).takeLast(3),
                3, 4, 5
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 2).takeLast(3),
                1, 2
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty().takeLast(3)
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .takeLast(3)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .takeLast(3)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void normal() {
        Folyam.range(1, 5).takeLast(3)
                .test(0L)
                .assertEmpty()
                .requestMore(Long.MAX_VALUE)
                .assertResult(3, 4, 5);

    }


    @Test
    public void normal2() {
        Folyam.range(1, 5).takeLast(3)
                .test(1L)
                .assertValues(3)
                .requestMore(2)
                .assertResult(3, 4, 5);

    }
}
