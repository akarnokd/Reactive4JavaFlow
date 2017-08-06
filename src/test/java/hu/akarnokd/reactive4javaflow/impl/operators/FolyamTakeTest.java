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

public class FolyamTakeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 7)
                        .take(5),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 7).hide()
                        .take(5),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.range(1, 7)
                        .take(5)
                        .filter(v -> true),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardConditionalHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 7).hide()
                        .take(5)
                        .filter(v -> true),
                1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .take(5)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .take(5)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void takeZero() {
        Folyam.range(1, 5)
                .take(0)
                .test()
                .assertResult();
    }

    @Test
    public void takeZeroConditional() {
        Folyam.range(1, 5)
                .take(0)
                .filter(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void standardMore() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .take(7),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardMoreConditional() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .take(7)
                        .filter(v -> true),
                1, 2, 3, 4, 5);
    }
}
