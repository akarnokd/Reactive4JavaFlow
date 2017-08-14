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

public class ParallelSumTest {

    @Test
    public void sumIntEmpty() {
        Folyam.empty()
        .parallel()
        .sumInt(v -> 0)
        .test()
        .assertResult();
    }


    @Test
    public void sumInt() {
        Folyam.range(1, 1000)
                .parallel()
                .sumInt(v -> v)
                .test()
                .assertResult(1001 * 500);
    }

    @Test
    public void sumIntSelectorCrash() {
        Folyam.range(1, 2)
                .parallel(2)
                .sumInt(v -> {
                    if (v == 2) {
                        throw new IOException();
                    }
                    return v;
                })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumIntSameError() {
        Folyam.error(new IOException())
                .parallel()
                .sumInt(v -> 0)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumIntMultipleErrors() {
        ParallelFolyam.fromArray(Folyam.error(new IOException()), Folyam.error(new IllegalArgumentException()))
                .sumInt(v -> 0)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void sumIntMultipleErrors3() {
        ParallelFolyam.fromArray(
                Folyam.error(new IOException()),
                Folyam.error(new IllegalArgumentException()),
                Folyam.error(new NullPointerException())
        )
                .sumInt(v -> 0)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                    TestHelper.assertError(errors, 2, NullPointerException.class);
                });
    }

    @Test
    public void sumIntCancel() {
        Folyam.range(1, 1000)
                .parallel()
                .sumInt(v -> 0)
                .test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void sumLongEmpty() {
        Folyam.empty()
                .parallel()
                .sumLong(v -> 0)
                .test()
                .assertResult();
    }


    @Test
    public void sumLong() {
        Folyam.range(1, 1000)
                .parallel()
                .sumLong(v -> v)
                .test()
                .assertResult(1001 * 500L);
    }

    @Test
    public void sumLongSelectorCrash() {
        Folyam.range(1, 2)
                .parallel(2)
                .sumLong(v -> {
                    if (v == 2) {
                        throw new IOException();
                    }
                    return v;
                })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumLongSameError() {
        Folyam.error(new IOException())
                .parallel()
                .sumLong(v -> 0L)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumLongMultipleErrors() {
        ParallelFolyam.fromArray(Folyam.error(new IOException()), Folyam.error(new IllegalArgumentException()))
                .sumLong(v -> 0L)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void sumLongMultipleErrors3() {
        ParallelFolyam.fromArray(
                Folyam.error(new IOException()),
                Folyam.error(new IllegalArgumentException()),
                Folyam.error(new NullPointerException())
        )
                .sumLong(v -> 0L)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                    TestHelper.assertError(errors, 2, NullPointerException.class);
                });
    }

    @Test
    public void sumLongCancel() {
        Folyam.range(1, 1000)
                .parallel()
                .sumLong(v -> 0L)
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void sumFloatEmpty() {
        Folyam.empty()
                .parallel()
                .sumFloat(v -> 0.5f)
                .test()
                .assertResult();
    }


    @Test
    public void sumFloat() {
        Folyam.range(1, 1000)
                .parallel()
                .sumFloat(v -> v + 0.5f)
                .test()
                .assertResult(1002 * 500f);
    }

    @Test
    public void sumFloatSelectorCrash() {
        Folyam.range(1, 2)
                .parallel(2)
                .sumFloat(v -> {
                    if (v == 2) {
                        throw new IOException();
                    }
                    return v + 0.5f;
                })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumFloatSameError() {
        Folyam.error(new IOException())
                .parallel()
                .sumFloat(v -> 0.5f)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumFloatMultipleErrors() {
        ParallelFolyam.fromArray(Folyam.error(new IOException()), Folyam.error(new IllegalArgumentException()))
                .sumFloat(v -> 0.5f)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void sumFloatMultipleErrors3() {
        ParallelFolyam.fromArray(
                Folyam.error(new IOException()),
                Folyam.error(new IllegalArgumentException()),
                Folyam.error(new NullPointerException())
        )
                .sumFloat(v -> 0.5f)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                    TestHelper.assertError(errors, 2, NullPointerException.class);
                });
    }

    @Test
    public void sumFloatCancel() {
        Folyam.range(1, 1000)
                .parallel()
                .sumFloat(v -> 0.5f)
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void sumDoubleEmpty() {
        Folyam.empty()
                .parallel()
                .sumDouble(v -> 0.5d)
                .test()
                .assertResult();
    }


    @Test
    public void sumDouble() {
        Folyam.range(1, 1000)
                .parallel()
                .sumDouble(v -> v + 0.5d)
                .test()
                .assertResult(1002 * 500d);
    }

    @Test
    public void sumDoubleSelectorCrash() {
        Folyam.range(1, 2)
                .parallel(2)
                .sumDouble(v -> {
                    if (v == 2) {
                        throw new IOException();
                    }
                    return v + 0.5d;
                })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumDoubleSameError() {
        Folyam.error(new IOException())
                .parallel()
                .sumDouble(v -> 0.5d)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void sumDoubleMultipleErrors() {
        ParallelFolyam.fromArray(Folyam.error(new IOException()), Folyam.error(new IllegalArgumentException()))
                .sumDouble(v -> 0.5d)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void sumDoubleMultipleErrors3() {
        ParallelFolyam.fromArray(
                Folyam.error(new IOException()),
                Folyam.error(new IllegalArgumentException()),
                Folyam.error(new NullPointerException())
        )
                .sumDouble(v -> 0.5d)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                    TestHelper.assertError(errors, 2, NullPointerException.class);
                });
    }

    @Test
    public void sumDoubleCancel() {
        Folyam.range(1, 1000)
                .parallel()
                .sumDouble(v -> 0.5d)
                .test(0, true, 0)
                .assertEmpty();
    }
}
