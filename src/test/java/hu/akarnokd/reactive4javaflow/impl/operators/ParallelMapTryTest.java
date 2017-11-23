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

public class ParallelMapTryTest implements CheckedConsumer<Object> {

    volatile int calls;

    @Override
    public void accept(Object t) throws Exception {
        calls++;
    }

    @Test
    public void mapNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .map(v -> v, e)
            .sequential()
            .test()
            .assertResult(1);
        }
    }
    @Test
    public void mapErrorNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .map(v -> v, e)
            .sequential()
            .test()
            .assertFailure(IOException.class);
        }
    }

    @Test
    public void mapConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.just(1)
            .parallel(1)
            .map(v -> v, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertResult(1);
        }
    }
    @Test
    public void mapErrorConditionalNoError() {
        for (ParallelFailureHandling e : ParallelFailureHandling.values()) {
            Folyam.<Integer>error(new IOException())
            .parallel(1)
            .map(v -> v, e)
            .filter(v -> true)
            .sequential()
            .test()
            .assertFailure(IOException.class);
        }
    }

    @Test
    public void mapFailWithError() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.ERROR)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void mapFailWithStop() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.STOP)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void mapFailWithRetry() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(new CheckedFunction<Integer, Integer>() {
            int count;
            @Override
            public Integer apply(Integer v) throws Exception {
                if (count++ == 1) {
                    return -1;
                }
                return 1 / v;
            }
        }, ParallelFailureHandling.RETRY)
        .sequential()
        .test()
        .assertResult(-1, 1);
    }

    @Test
    public void mapFailWithRetryLimited() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void mapFailWithSkip() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.SKIP)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapFailHandlerThrows() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, (n, e) -> {
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
    public void mapWrongParallelism() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .map(v -> v, ParallelFailureHandling.ERROR)
        );
    }

    @Test
    public void mapInvalidSource() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .map(v -> v, ParallelFailureHandling.ERROR)
                    .sequential()
                    .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void mapFailWithErrorConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.ERROR)
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(ArithmeticException.class);
    }

    @Test
    public void mapFailWithStopConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.STOP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void mapFailWithRetryConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(new CheckedFunction<Integer, Integer>() {
            int count;
            @Override
            public Integer apply(Integer v) throws Exception {
                if (count++ == 1) {
                    return -1;
                }
                return 1 / v;
            }
        }, ParallelFailureHandling.RETRY)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(-1, 1);
    }

    @Test
    public void mapFailWithRetryLimitedConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, (n, e) -> n < 5 ? ParallelFailureHandling.RETRY : ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @Test
    public void mapFailWithSkipConditional() {
        Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, ParallelFailureHandling.SKIP)
        .filter(v -> true)
        .sequential()
        .test()
        .assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapFailHandlerThrowsConditional() {
        TestConsumer<Integer> ts = Folyam.range(0, 2)
        .parallel(1)
        .map(v -> 1 / v, (n, e) -> {
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
    public void mapWrongParallelismConditional() {
        TestHelper.checkInvalidParallelSubscribers(
            Folyam.just(1).parallel(1)
            .map(v -> v, ParallelFailureHandling.ERROR)
            .filter(v -> true)
        );
    }

    @Test
    public void mapInvalidSourceConditional() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
            .map(v -> v, ParallelFailureHandling.ERROR)
            .filter(v -> true)
            .sequential()
            .test();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void failureHandlingEnum() {
        TestHelper.checkEnum(ParallelFailureHandling.class);
    }
}
