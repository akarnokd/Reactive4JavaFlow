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

public class FolyamRetryTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.range(1, 5).retry(), 1, 2, 3, 4, 5);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(Folyam.range(1, 5).hide().retry(), 1, 2, 3, 4, 5);
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.range(1, 5).retry(1), 1, 2, 3, 4, 5);
    }

    @Test
    public void retry0() {
        Folyam.error(new IOException())
                .retry(0)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1() {
        Folyam.error(new IOException())
                .retry(1)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1Conditional() {
        Folyam.error(new IOException())
                .retry(1)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void retry2() {
        int[] c = { 0 };

        Folyam.defer(() -> {
            if (c[0]++ == 1) {
                return Folyam.range(1, 5);
            }
            return Folyam.error(new IOException());
        })
        .retry(2)
        .test()
        .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void retry2Fail() {
        int[] c = { 0 };

        Folyam.defer(() -> {
            if (c[0]++ == 2) {
                return Folyam.range(1, 5);
            }
            return Folyam.error(new IOException());
        })
                .retry(2)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1Limit() {
        int[] c = { 0 };

        Folyam.defer(() -> {
            if (c[0]++ == 5) {
                return Folyam.range(1, 5);
            }
            return Folyam.error(new IOException());
        })
                .retry(1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void retryPredicate() {
        int[] c = { 0 };
        Folyam.defer(() -> {
            if (c[0]++ == 5) {
                return Folyam.range(1, 5);
            }
            return new FolyamError(new IOException());
        })
        .retry(e -> e instanceof IOException)
        .test()
        .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void retryPredicateNotMaching() {
        int[] c = { 0 };
        Folyam.defer(() -> {
            if (c[0]++ == 5) {
                return Folyam.range(1, 5);
            }
            return new FolyamError(new IOException());
        })
                .retry(5, e -> e instanceof RuntimeException)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrash() {
        Folyam.error(new IOException("Outer"))
        .retry(e -> { throw new IOException("Inner"); })
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "Outer");
                    TestHelper.assertError(errors, 1, IOException.class, "Inner");
                });
    }

    @Test
    public void cancelUpfront() {
        Folyam.range(1, 5)
                .retry()
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void retryAbiter() {
        int[] c = { 0 };
        Folyam.range(1, 3)
                .concatWith(Folyam.fromCallable(() -> {
                    if (c[0]++ == 1) {
                        return 4;
                    }
                    throw new IOException();
                }))
                .retry()
                .test()
                .assertResult(1, 2, 3, 1, 2, 3, 4);
    }
}
