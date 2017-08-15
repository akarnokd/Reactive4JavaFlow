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

public class EsetlegRetryTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Esetleg.just(1).retry(), 1);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(Esetleg.just(1).hide().retry(), 1);
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Esetleg.just(1).retry(1), 1);
    }

    @Test
    public void retry0() {
        Esetleg.error(new IOException())
                .retry(0)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1() {
        Esetleg.error(new IOException())
                .retry(1)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1Conditional() {
        Esetleg.error(new IOException())
                .retry(1)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void retry2() {
        int[] c = { 0 };

        Esetleg.defer(() -> {
            if (c[0]++ == 1) {
                return Esetleg.just(1);
            }
            return Esetleg.error(new IOException());
        })
        .retry(2)
        .test()
        .assertResult(1);
    }

    @Test
    public void retry2Fail() {
        int[] c = { 0 };

        Esetleg.defer(() -> {
            if (c[0]++ == 2) {
                return Esetleg.just(1);
            }
            return Esetleg.error(new IOException());
        })
                .retry(2)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void retry1Limit() {
        int[] c = { 0 };

        Esetleg.defer(() -> {
            if (c[0]++ == 5) {
                return Esetleg.just(1);
            }
            return Esetleg.error(new IOException());
        })
                .retry(1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void retryPredicate() {
        int[] c = { 0 };
        Esetleg.defer(() -> {
            if (c[0]++ == 5) {
                return Esetleg.just(1);
            }
            return new EsetlegError<>(new IOException());
        })
        .retry(e -> e instanceof IOException)
        .test()
        .assertResult(1);
    }

    @Test
    public void retryPredicateNotMaching() {
        int[] c = { 0 };
        Esetleg.defer(() -> {
            if (c[0]++ == 5) {
                return Esetleg.just(1);
            }
            return new EsetlegError<>(new IOException());
        })
                .retry(5, e -> e instanceof RuntimeException)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrash() {
        Esetleg.error(new IOException("Outer"))
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
        Esetleg.just(1)
                .retry()
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void retryArbiter() {
        int[] c = { 0 };
        Esetleg.just(1)
                .map(v -> {
                    if (c[0]++ == 1) {
                        return 4;
                    }
                    throw new IOException();
                })
                .retry()
                .test()
                .assertResult(4);
    }
}
