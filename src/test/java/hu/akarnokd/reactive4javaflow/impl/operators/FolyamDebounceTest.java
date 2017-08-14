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
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FolyamDebounceTest {

    @Test
    public void normal() {

        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.debounce(v -> other).test(1);

        dp.onNext(1);

        assertTrue(other.hasSubscribers());

        tc.assertEmpty();

        dp.onNext(2);
        dp.onNext(3);

        tc.assertEmpty();

        other.onNext(100);

        tc.assertValues(3);

        assertFalse(other.hasSubscribers());

        tc.requestMore(1);

        dp.onNext(4);

        dp.onComplete();

        tc.assertResult(3, 4);
    }


    @Test
    public void normalNoLast() {

        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.debounce(v -> other).test(1);

        dp.onNext(1);

        assertTrue(other.hasSubscribers());

        tc.assertEmpty();

        dp.onNext(2);
        dp.onNext(3);

        tc.assertEmpty();

        other.onNext(100);

        tc.assertValues(3);

        assertFalse(other.hasSubscribers());

        tc.requestMore(1);

        dp.onComplete();

        tc.assertResult(3);
    }

    @Test
    public void mainError() {
        TestHelper.assertFailureComposed(-1,
                f -> f.debounce(v -> Folyam.never()), IOException.class);
    }

    @Test
    public void debouncerError() {
        TestHelper.assertFailureComposed(-1,
                f -> Folyam.just(1).debounce(v -> f), IOException.class);
    }

    @Test
    public void debouncerCrash() {
        TestHelper.assertFailureComposed(5,
                f -> Folyam.just(1).debounce(v -> { throw new IOException(); }), IOException.class);
    }


    @Test
    public void mainErrorsAfter1() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.debounce(v -> other).test();

        dp.onNext(1);
        dp.onError(new IOException());

        assertFalse(other.hasSubscribers());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void cancel() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.debounce(v -> other).test();

        dp.onNext(1);

        tc.cancel();

        assertFalse(dp.hasSubscribers());
        assertFalse(other.hasSubscribers());
    }

    @Test
    public void debounceWithEmpty() {
        Folyam.range(1, 5)
                .debounce(v -> Folyam.empty())
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }


    @Test
    @Ignore("Work out backpressure") // FIXME work out backpressure
    public void debounceImmediately() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .debounce(v -> Folyam.empty()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void throttleWithTimeout() {
        TestSchedulerService executor = new TestSchedulerService();

        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.throttleWithTimeout(10, TimeUnit.MILLISECONDS, executor).test();

        dp.onNext(1);
        dp.onNext(2);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2);

        dp.onNext(3);
        dp.onNext(4);
        dp.onNext(5);

        executor.advanceTimeBy(5, TimeUnit.MILLISECONDS);

        dp.onNext(6);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2, 6);

        dp.onNext(7);

        executor.advanceTimeBy(5, TimeUnit.MILLISECONDS);

        dp.onComplete();

        tc.assertResult(2, 6, 7);

        assertEquals(0, executor.activeWorkers());
    }
}
