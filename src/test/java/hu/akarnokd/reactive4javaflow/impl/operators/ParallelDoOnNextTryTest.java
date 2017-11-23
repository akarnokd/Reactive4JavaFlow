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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.*;
import org.junit.Test;

public class ParallelDoOnNextTryTest implements CheckedConsumer<Object> {

    volatile int calls;

    @Override
    public void accept(Object t) throws Exception {
        calls++;
    }

    @Test
    public void doOnNextNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .doOnNext(this, e)
            .sequential()
            .test()
            .assertResult(1);

            assertEquals(calls, 1);
            calls = 0;
        }
    }
    @Test
    public void doOnNextErrorNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .doOnNext(this, e)
            .sequential()
            .test()
            .assertFailure(IOException.class);

            assertEquals(calls, 0);
        }
    }

    @Test
    public void doOnNextConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .doOnNext(this, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertResult(1);

            assertEquals(calls, 1);
            calls = 0;
        }
    }

    @Test
    public void doOnNextErrorConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .doOnNext(this, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertFailure(IOException.class);

            assertEquals(calls, 0);
        }
    }

    @Test
    public void doOnNextFailWithError() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.ERROR)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void doOnNextFailWithStop() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.STOP)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void doOnNextFailWithRetry() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(new CheckedConsumer<>() {
            int count;

            @Override
            public void accept(Integer v) throws Exception {
                if (count++ == 1) {
                    return;
                }
                if (1 / v < 0) {
                    System.out.println("Should not happen!");
                }
            }
        }, ParallelFailureHandling.RETRY)
        .sequential()
        .test()
        .assertResult(0, 1);
    }

    @Test
    public void doOnNextFailWithRetryLimited() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void doOnNextFailWithSkip() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doOnNextFailHandlerThrows() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, (n, e) -> {
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
    public void doOnNextWrongParallelism() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .doOnNext(e -> { }, ParallelFailureHandling.ERROR)
        );
    }

    @Test
    public void filterInvalidSource() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .doOnNext(e -> { }, ParallelFailureHandling.ERROR)
                    .sequential()
                    .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void doOnNextFailWithErrorConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.ERROR)
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void doOnNextFailWithStopConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.STOP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void doOnNextFailWithRetryConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(new CheckedConsumer<>() {
            int count;

            @Override
            public void accept(Integer v) throws Exception {
                if (count++ == 1) {
                    return;
                }
                if (1 / v < 0) {
                    System.out.println("Should not happen!");
                }
            }
        }, ParallelFailureHandling.RETRY)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(0, 1);
    }

    @Test
    public void doOnNextFailWithRetryLimitedConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void doOnNextFailWithSkipConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doOnNextFailHandlerThrowsConditional() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .doOnNext(v -> {
            if (1 / v < 0) {
                System.out.println("Should not happen!");
            }
        }, (n, e) -> {
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
    public void doOnNextWrongParallelismConditional() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .doOnNext(v -> { }, ParallelFailureHandling.ERROR)
            .filter(v -> true)
        );
    }

    @Test
    public void filterInvalidSourceConditional() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
            .doOnNext(e -> { }, ParallelFailureHandling.ERROR)
            .filter(v -> true)
            .sequential()
            .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }
}
