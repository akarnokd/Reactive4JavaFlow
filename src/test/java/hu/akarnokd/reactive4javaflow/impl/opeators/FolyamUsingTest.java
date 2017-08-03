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
import static org.junit.Assert.assertTrue;

public class FolyamUsingTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.using(() -> 0, a -> Folyam.range(1, 5), a -> { }),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.using(() -> 0, a -> Folyam.range(1, 5).hide(), a -> { }),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.using(() -> 0, a -> Folyam.range(1, 5), a -> { })
                .filter(v -> true),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardConditiona2() {
        TestHelper.assertResult(
                Folyam.using(() -> 0, a -> Folyam.range(1, 5).hide(), a -> { })
                .filter(v -> true),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void eagerEmpty() {
        Folyam.using(() -> 1, a -> Folyam.empty(), a -> { }, true)
                .test()
                .assertResult();
    }


    @Test
    public void noneagerEmpty() {
        Folyam.using(() -> 1, a -> Folyam.empty(), a -> { }, false)
                .test()
                .assertResult();
    }


    @Test
    public void eagerEmptyConditional() {
        Folyam.using(() -> 1, a -> Folyam.empty(), a -> { }, true)
                .filter(v -> true)
                .test()
                .assertResult();
    }


    @Test
    public void noneagerEmptyConditional() {
        Folyam.using(() -> 1, a -> Folyam.empty(), a -> { }, false)
                .filter(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void resourceSupplierThrows() {
        Folyam.using(() -> { throw new IOException(); }, a -> Folyam.range(1, 5), a -> { })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerFlowSupplierThrows() {
        int[] value = { 0 };
        Folyam.using(() -> 1, a -> { throw new IOException(); }, a -> value[0] = a, true)
        .test()
        .assertFailure(IOException.class);

        assertEquals(1, value[0]);
    }

    @Test
    public void noneagerFlowSupplierThrows() {
        int[] value = { 0 };
        Folyam.using(() -> 1, a -> { throw new IOException(); }, a -> value[0] = a, false)
                .test()
                .assertFailure(IOException.class);

        assertEquals(1, value[0]);
    }

    @Test
    public void eagerFlowSupplierCleanupThrow() {
        Folyam.using(() -> 1, a -> { throw new IOException("1"); }, a -> { throw new IOException("2"); }, true)
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
            Folyam.using(() -> 1, a -> { throw new IOException("1"); }, a -> { throw new IOException("2"); }, false)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void eagerError() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException()), r -> { }, true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerErrorConditional() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException()), r -> { }, true)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void noneagerError() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException()), r -> { }, false)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void noneagerErrorConditional() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException()), r -> { }, false)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void eagerErrorCleanupThrows() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException("1")), r -> { throw new IOException("2"); }, true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "1");
                    TestHelper.assertError(errors, 1, IOException.class, "2");
                });
    }

    @Test
    public void eagerCompleteCleanupThrows() {
        Folyam.using(() -> null, a -> Folyam.empty(), r -> { throw new IOException("2"); }, true)
                .test()
                .assertFailureAndMessage(IOException.class, "2");
    }

    @Test
    public void eagerErrorCleanupThrowsConditional() {
        Folyam.using(() -> null, a -> Folyam.error(new IOException("1")), r -> { throw new IOException("2"); }, true)
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
        Folyam.using(() -> null, a -> Folyam.empty(), r -> { throw new IOException("2"); }, true)
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IOException.class, "2");
    }

    @Test
    public void noneagerErrorCleanupThrow() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.using(() -> null, a -> Folyam.error(new IOException("1")), r -> { throw new IOException("2"); }, false)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void noneagerErrorCleanupThrowConditional() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.using(() -> null, a -> Folyam.error(new IOException("1")), r -> { throw new IOException("2"); }, false)
                    .filter(v -> true)
                    .test()
                    .assertFailureAndMessage(IOException.class, "1");

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }


    @Test
    public void noneagerEmptyCleanupThrow() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.using(() -> null, a -> Folyam.empty(), r -> { throw new IOException("2"); }, false)
                    .test()
                    .assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void noneagerEmptyCleanupThrowConditional() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.using(() -> null, a -> Folyam.empty(), r -> { throw new IOException("2"); }, false)
                    .filter(v -> true)
                    .test()
                    .assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "2");
        });
    }

    @Test
    public void eagerCancel() {
        int[] value = { 0 };
        Folyam.using(() -> 1, a -> Folyam.range(1, 5), a -> value[0] = a, true)
                .take(1)
                .test()
                .assertResult(1)
        ;

        assertEquals(1, value[0]);
    }

    @Test
    public void noneagerCancel() {
        int[] value = { 0 };
        Folyam.using(() -> 1, a -> Folyam.range(1, 5), a -> value[0] = a, false)
                .take(1)
                .test()
                .assertResult(1)
        ;

        assertEquals(1, value[0]);
    }
}
