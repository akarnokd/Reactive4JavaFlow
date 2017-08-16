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

public class FolyamSpanoutTest {

    @Test
    public void normal() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        ts.assertEmpty();

        pp.onNext(1);
        pp.onNext(2);

        ts.assertEmpty();

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1);

        pp.onNext(3);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2, 3);

        pp.onComplete();

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertResult(1, 2, 3);
    }

    @Test
    public void normalInitialDelay() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, 100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        ts.assertEmpty();

        pp.onNext(1);
        pp.onNext(2);

        ts.assertEmpty();

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertEmpty();

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1);

        pp.onNext(3);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2, 3);

        pp.onComplete();

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertResult(1, 2, 3);
    }

    @Test
    public void errorImmediate() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onNext(1);
        pp.onError(new IOException());

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.assertFailure(IOException.class);
    }

    @Test
    public void errorDelayed() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanoutDelayError(100L, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onNext(1);
        pp.onError(new IOException());

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.assertFailure(IOException.class, 1);
    }

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .spanout(10L, TimeUnit.MILLISECONDS, SchedulerServices.single()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void normal2() {
        Folyam.range(1, 3)
                .spanout(100L, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normal3() {
        Folyam.range(1, 3)
                .spanoutDelayError(100L, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normal4() {
        Folyam.range(1, 3)
                .spanout(100L, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normal5() {
        Folyam.range(1, 3)
                .spanoutDelayError(100L, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normalInitial2() {
        Folyam.range(1, 3)
                .spanout(50L, 100L, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normalInitial3() {
        Folyam.range(1, 3)
                .spanoutDelayError(50L, 100L, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normalInitial4() {
        Folyam.range(1, 3)
                .spanout(50L, 100L, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normalInitial5() {
        Folyam.range(1, 3)
                .spanoutDelayError(50L, 100L, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3);
    }

    @Test
    public void take() {
        Folyam.range(1, 3)
                .spanout(100L, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .take(2)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2);
    }

    @Test
    public void emitWayAfterPrevious() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onNext(1);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.assertValues(1);

        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);

        pp.onNext(2);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertValues(1, 2);
    }

    @Test
    public void empty() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onComplete();

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertResult();
    }

    @Test
    public void errorImmediateWithoutValue() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanout(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onError(new IOException());

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertFailure(IOException.class);
    }

    @Test
    public void errorDelayErrorWithoutValue() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .spanoutDelayError(100, TimeUnit.MILLISECONDS, scheduler)
                .test();

        pp.onError(new IOException());

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ts.assertFailure(IOException.class);
    }
}
