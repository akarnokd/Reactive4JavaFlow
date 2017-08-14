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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import org.junit.Test;

import java.io.IOException;

public class ParallelIgnoreElementsTest {

    @Test
    public void normal() {
        Folyam.range(1, 1000)
                .parallel()
                .ignoreElements()
                .test()
                .assertResult();
    }

    @Test
    public void sameError() {
        Folyam.error(new IOException())
                .parallel()
                .ignoreElements()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void multipleErrors() {
        ParallelFolyam.fromArray(Folyam.error(new IOException()), Folyam.error(new IllegalArgumentException()))
                .ignoreElements()
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void multipleErrors3() {
        ParallelFolyam.fromArray(
                Folyam.error(new IOException()),
                Folyam.error(new IllegalArgumentException()),
                Folyam.error(new NullPointerException())
        )
                .ignoreElements()
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                    TestHelper.assertError(errors, 2, NullPointerException.class);
                });
    }

    @Test
    public void cancel() {
        Folyam.range(1, 1000)
                .parallel()
                .ignoreElements()
                .test(0, true, 0)
                .assertEmpty();
    }
}
