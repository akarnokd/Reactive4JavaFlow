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

public class FolyamSkipTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 7).skip(2),
                3, 4, 5, 6, 7);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 7).hide().skip(2),
                3, 4, 5, 6, 7);
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.range(1, 7).skip(2)
                .filter(v -> true),
                3, 4, 5, 6, 7);
    }

    @Test
    public void standardHiddenConditional() {
        TestHelper.assertResult(
                Folyam.range(1, 7).hide().skip(2)
                .filter(v -> true),
                3, 4, 5, 6, 7);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .skip(2)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .skip(2)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void skipAll() {
        TestHelper.assertResult(
                Folyam.range(1, 7).skip(7));
    }

    @Test
    public void skipNone() {
        Folyam.range(1, 5).skip(0L)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }
}
