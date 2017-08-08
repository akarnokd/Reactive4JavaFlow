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

public class FolyamSkipUntilTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).skipUntil(Folyam.just(1))
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().skipUntil(Folyam.just(1))
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5).skipUntil(Folyam.range(1, 3))
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).skipUntil(Folyam.empty())
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).skipUntil(Folyam.never())
        );
    }

    @Test
    public void standard3Hidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().skipUntil(Folyam.never())
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException()).skipUntil(Folyam.never())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException()).skipUntil(Folyam.never())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOther() {
        Folyam.just(1).skipUntil(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOtherConditional() {
        Folyam.just(1).skipUntil(Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
