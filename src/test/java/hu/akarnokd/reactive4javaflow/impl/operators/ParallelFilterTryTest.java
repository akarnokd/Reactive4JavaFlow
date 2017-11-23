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

import java.io.IOException;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.*;
import org.junit.Test;

public class ParallelFilterTryTest implements CheckedConsumer<Object> {

    volatile int calls;

    @Override
    public void accept(Object t) throws Exception {
        calls++;
    }

    @Test
    public void filterNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .filter(v -> true, e)
            .sequential()
            .test()
            .assertResult(1);
        }
    }

    @Test
    public void filterFalse() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .filter(v -> false, e)
            .sequential()
            .test()
            .assertResult();
        }
    }

    @Test
    public void filterFalseConditional() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .filter(v -> false, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertResult();
        }
    }

    @Test
    public void filterErrorNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .filter(v -> true, e)
            .sequential()
            .test()
            .assertFailure(IOException.class);
        }
    }

    @Test
    public void filterConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .filter(v -> true, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertResult(1);
        }
    }
    @Test
    public void filterErrorConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .filter(v -> true, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertFailure(IOException.class);
        }
    }

    @Test
    public void filterFailWithError() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.ERROR)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void filterFailWithStop() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.STOP)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void filterFailWithRetry() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(new CheckedPredicate<>() {
            int count;

            @Override
            public boolean test(Integer v) throws Exception {
                if (count++ == 1) {
                    return true;
                }
                return 1 / v > 0;
            }
        }, ParallelFailureHandling.RETRY)
        .sequential()
        .test()
        .assertResult(0, 1);
    }

    @Test
    public void filterFailWithRetryLimited() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void filterFailWithSkip() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void filterFailHandlerThrows() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, (n, e) -> {
            throw new IOException();
        })
        .sequential()
        .test()
        .assertFailure(CompositeThrowable.class)
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, ArithmeticException.class);
            TestHelper.assertError(errors, 1, IOException.class);
        });

    }

    @Test
    public void filterWrongParallelism() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .filter(v -> true, ParallelFailureHandling.ERROR)
        );
    }

    @Test
    public void filterInvalidSource() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .filter(v -> true, ParallelFailureHandling.ERROR)
                    .sequential()
                    .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void filterFailWithErrorConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.ERROR)
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void filterFailWithStopConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.STOP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void filterFailWithRetryConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(new CheckedPredicate<>() {
            int count;

            @Override
            public boolean test(Integer v) throws Exception {
                if (count++ == 1) {
                    return true;
                }
                return 1 / v > 0;
            }
        }, ParallelFailureHandling.RETRY)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(0, 1);
    }

    @Test
    public void filterFailWithRetryLimitedConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void filterFailWithSkipConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void filterFailHandlerThrowsConditional() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .filter(v -> 1 / v > 0, (n, e) -> {
            throw new IOException();
        })
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(CompositeThrowable.class)
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, ArithmeticException.class);
            TestHelper.assertError(errors, 1, IOException.class);
        });
    }

    @Test
    public void filterWrongParallelismConditional() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .filter(v -> true, ParallelFailureHandling.ERROR)
            .filter(v -> true)
        );
    }

    @Test
    public void filterInvalidSourceConditional() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .filter(v -> true, ParallelFailureHandling.ERROR)
                    .filter(v -> true)
                    .sequential()
                    .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }
}
