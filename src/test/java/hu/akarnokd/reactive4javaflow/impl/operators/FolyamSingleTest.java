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
import java.util.NoSuchElementException;

public class FolyamSingleTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.just(1).single(), 1
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.just(1).hide().single(), 1
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.just(1).esetleg(), 1
        );
    }

    @Test
    public void standard2Hidden() {
        TestHelper.assertResult(
                Folyam.just(1).hide().esetleg(), 1
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.empty().esetleg()
        );
    }

    @Test
    public void standard3Hidden() {
        TestHelper.assertResult(
                Folyam.empty().hide().esetleg()
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .single()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error2() {
        Folyam.error(new IOException())
                .esetleg()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void more() {
        Folyam.range(1, 5)
                .single()
                .test()
                .assertFailure(IndexOutOfBoundsException.class);
    }

    @Test
    public void more2() {
        Folyam.range(1, 5)
                .esetleg()
                .test()
                .assertFailure(IndexOutOfBoundsException.class);
    }

    @Test
    public void empty() {
        Folyam.empty().single()
                .test()
                .assertFailure(NoSuchElementException.class);
    }

    @Test
    public void emptyHidden() {
        Folyam.empty().hide().single()
                .test()
                .assertFailure(NoSuchElementException.class);
    }
}
