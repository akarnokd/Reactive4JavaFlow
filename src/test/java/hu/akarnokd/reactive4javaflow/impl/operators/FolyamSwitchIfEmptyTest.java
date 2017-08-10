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

public class FolyamSwitchIfEmptyTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).switchIfEmpty(Folyam.range(6, 5)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .switchIfEmpty(Folyam.range(6, 5)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty().switchIfEmpty(Folyam.range(6, 5)),
                6, 7, 8, 9, 10);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).defaultIfEmpty(6),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.empty().defaultIfEmpty(6),
                6);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .switchIfEmpty(Folyam.range(6, 5))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .switchIfEmpty(Folyam.range(6, 5))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorOther() {
        Folyam.empty()
                .switchIfEmpty(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOtherConditional() {
        Folyam.empty()
                .switchIfEmpty(Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
