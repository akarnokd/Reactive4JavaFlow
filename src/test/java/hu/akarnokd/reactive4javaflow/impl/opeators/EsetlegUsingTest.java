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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class EsetlegUsingTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.using(() -> 0, a -> Esetleg.just(1), a -> { }),
                1
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.using(() -> 0, a -> Esetleg.just(1).hide(), a -> { }),
                1
        );
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Esetleg.using(() -> 0, a -> Esetleg.just(1), a -> { })
                .filter(v -> true),
                1
        );
    }

    @Test
    public void standardConditiona2() {
        TestHelper.assertResult(
                Esetleg.using(() -> 0, a -> Esetleg.just(1).hide(), a -> { })
                .filter(v -> true),
                1
        );
    }

    @Test
    public void eagerEmpty() {
        Esetleg.using(() -> 1, a -> Esetleg.empty(), a -> { }, true)
                .test()
                .assertResult();
    }


    @Test
    public void noneagerEmpty() {
        Esetleg.using(() -> 1, a -> Esetleg.empty(), a -> { }, false)
                .test()
                .assertResult();
    }


    @Test
    public void eagerEmptyConditional() {
        Esetleg.using(() -> 1, a -> Esetleg.empty(), a -> { }, true)
                .filter(v -> true)
                .test()
                .assertResult();
    }


    @Test
    public void noneagerEmptyConditional() {
        Esetleg.using(() -> 1, a -> Esetleg.empty(), a -> { }, false)
                .filter(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void resourceSupplierThrows() {
        Esetleg.using(() -> { throw new IOException(); }, a -> Esetleg.just(1), a -> { })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerFlowSupplierThrows() {
        int[] value = { 0 };
        Esetleg.using(() -> 1, a -> { throw new IOException(); }, a -> value[0] = a, true)
        .test()
        .assertFailure(IOException.class);

        assertEquals(1, value[0]);
    }

    @Test
    public void noneagerFlowSupplierThrows() {
        int[] value = { 0 };
        Esetleg.using(() -> 1, a -> { throw new IOException(); }, a -> value[0] = a, false)
                .test()
                .assertFailure(IOException.class);

        assertEquals(1, value[0]);
    }

    @Test
    public void eagerFlowSupplierCleanupThrow() {
        Esetleg.using(() -> 1, a -> { throw new IOException("1"); }, a -> { throw new IOException("2"); }, true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "1");
                    TestHelper.assertError(errors, 1, IOException.class, "2");
                });
    }

    @Test
    public void noneagerFlowSupplierCleanupThrow() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.using(() -> 1, a -> { throw new IOException("1"); }, a -> { throw new IOException("2"); }, false)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void eagerError() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException()), r -> { }, true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerErrorConditional() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException()), r -> { }, true)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void noneagerError() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException()), r -> { }, false)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void noneagerErrorConditional() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException()), r -> { }, false)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerErrorCleanupThrows() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException("1")), r -> { throw new IOException("2"); }, true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "1");
                    TestHelper.assertError(errors, 1, IOException.class, "2");
                });
    }

    @Test
    public void eagerCompleteCleanupThrows() {
        Esetleg.using(() -> null, a -> Esetleg.empty(), r -> { throw new IOException("2"); }, true)
                .test()
                .assertFailureAndMessage(IOException.class, "2");
    }

    @Test
    public void eagerErrorCleanupThrowsConditional() {
        Esetleg.using(() -> null, a -> Esetleg.error(new IOException("1")), r -> { throw new IOException("2"); }, true)
                .filter(v -> true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "1");
                    TestHelper.assertError(errors, 1, IOException.class, "2");
                });
    }

    @Test
    public void eagerCompleteCleanupThrowsConditional() {
        Esetleg.using(() -> null, a -> Esetleg.empty(), r -> { throw new IOException("2"); }, true)
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IOException.class, "2");
    }

    @Test
    public void noneagerErrorCleanupThrow() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.using(() -> null, a -> Esetleg.error(new IOException("1")), r -> { throw new IOException("2"); }, false)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void noneagerErrorCleanupThrowConditional() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.using(() -> null, a -> Esetleg.error(new IOException("1")), r -> { throw new IOException("2"); }, false)
                    .filter(v -> true)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }


    @Test
    public void noneagerEmptyCleanupThrow() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.using(() -> null, a -> Esetleg.empty(), r -> { throw new IOException("2"); }, false)
                    .test()
                    .assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void noneagerEmptyCleanupThrowConditional() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.using(() -> null, a -> Esetleg.empty(), r -> { throw new IOException("2"); }, false)
                    .filter(v -> true)
                    .test()
                    .assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void eagerCancel() {
        int[] value = { 0 };
        Esetleg.using(() -> 1, a -> Esetleg.just(1), a -> value[0] = a, true)
                .test(0, true, 0)
                .assertEmpty()
        ;

        assertEquals(1, value[0]);
    }

    @Test
    public void noneagerCancel() {
        int[] value = { 0 };
        Esetleg.using(() -> 1, a -> Esetleg.just(1), a -> value[0] = a, false)
                .test(0, true, 0)
                .assertEmpty()
        ;

        assertEquals(1, value[0]);
    }
}
