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
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FolyamSampleTest {

    @Test
    public void normalEmitLast() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.sample(other).test();

        tc.assertEmpty();

        other.onNext(100);

        tc.assertEmpty();

        dp.onNext(1);

        tc.assertEmpty();

        dp.onNext(2);

        tc.assertEmpty();

        other.onNext(200);

        tc.assertValues(2);

        other.onNext(300);

        tc.assertValues(2);

        dp.onNext(3);

        other.onComplete();

        assertFalse(dp.hasSubscribers());

        tc.assertResult(2, 3);
    }

    @Test
    public void normalDropLast() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.sample(other, false).test();

        tc.assertEmpty();

        other.onNext(100);

        tc.assertEmpty();

        dp.onNext(1);

        tc.assertEmpty();

        dp.onNext(2);

        tc.assertEmpty();

        other.onNext(200);

        tc.assertValues(2);

        other.onNext(300);

        tc.assertValues(2);

        dp.onNext(3);

        other.onComplete();

        assertFalse(dp.hasSubscribers());

        tc.assertResult(2);
    }

    @Test
    public void throttleLast() {
        TestSchedulerService executor = new TestSchedulerService();

        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.throttleLast(10, TimeUnit.MILLISECONDS, executor)
                .test();

        dp.onNext(1);
        dp.onNext(2);

        tc.assertEmpty();

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2);

        dp.onNext(3);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2, 3);

        dp.onNext(4);
        dp.onComplete();

        tc.assertResult(2, 3, 4);

        assertEquals(0, executor.activeWorkers());
    }

    @Test
    public void throttleLastDontEmitLast() {
        TestSchedulerService executor = new TestSchedulerService();

        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.throttleLast(10, TimeUnit.MILLISECONDS, executor, false)
                .test();

        dp.onNext(1);
        dp.onNext(2);

        tc.assertEmpty();

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2);

        dp.onNext(3);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(2, 3);

        dp.onNext(4);
        dp.onComplete();

        tc.assertResult(2, 3);

        assertEquals(0, executor.activeWorkers());
    }

    @Test
    public void errorMain() {
        TestHelper.assertFailureComposed(-1,
                f -> f.sample(Folyam.never()), IOException.class);
    }

    @Test
    public void errorOther() {
        TestHelper.assertFailureComposed(-1,
                f -> Folyam.never().sample(f), IOException.class);
    }

    @Test
    public void take() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> other = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.sample(other).take(1).test();

        dp.onNext(1);
        other.onNext(100);

        tc.assertResult(1);

        assertFalse(dp.hasSubscribers());
        assertFalse(other.hasSubscribers());
    }
}
