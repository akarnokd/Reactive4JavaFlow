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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class FolyamOnBackpressureTimeoutTest implements CheckedConsumer<Object> {

    final List<Object> evicted = Collections.synchronizedList(new ArrayList<Object>());

    @Override
    public void accept(Object t) throws Exception {
        evicted.add(t);
    }

    @Test
    public void empty() {
        Folyam.<Integer>empty()
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .test()
                .assertResult();
    }

    @Test
    public void error() {
        Folyam.<Integer>error(new IOException())
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorDelayed() {
        TestConsumer<Integer> ts = Folyam.just(1).concatWith(Folyam.<Integer>error(new IOException()))
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .test(0);

        ts
                .assertEmpty()
                .requestMore(1)
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void normal1() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal1SingleStep() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .rebatchRequests(1)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal2() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal2SingleStep() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .rebatchRequests(1)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal3() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(10, 1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal3SingleStep() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(10, 1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .rebatchRequests(1)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal4() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal4SingleStep() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .rebatchRequests(1)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void bufferLimit() {
        TestConsumer<Integer> ts = Folyam.range(1, 5)
                .onBackpressureTimeout(1, 1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .test(0);

        ts
                .assertEmpty()
                .requestMore(1)
                .assertResult(5);

        assertEquals(Arrays.asList(1, 2, 3, 4), evicted);
    }

    @Test
    public void timeoutLimit() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .onBackpressureTimeout(1, 1, TimeUnit.SECONDS, scheduler, this)
                .test(0);

        ts.assertEmpty();

        pp.onNext(1);

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        pp.onNext(2);

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        pp.onNext(3);

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        pp.onNext(4);

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        pp.onNext(5);

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        pp.onComplete();

        ts
                .assertEmpty()
                .requestMore(1)
                .assertResult(5);

        assertEquals(Arrays.asList(1, 2, 3, 4), evicted);
    }

    @Test
    public void take() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                .take(2)
                .test()
                .assertResult(1, 2);
    }

    @Test
    public void cancelEvictAll() {
        TestConsumer<Integer> ts = Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .test(0);

        ts.cancel();

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), evicted);
    }

    @Test
    public void timeoutEvictAll() {
        TestSchedulerService scheduler = new TestSchedulerService();

        TestConsumer<Integer> ts = Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.SECONDS, scheduler, this)
                .test(0);

        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);

        ts
                .requestMore(1)
                .assertResult();

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), evicted);
    }

    @Test
    public void evictCancels() {
        TestSchedulerService scheduler = new TestSchedulerService();

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        final TestConsumer<Integer> ts = new TestConsumer<Integer>(0L);

        pp
                .onBackpressureTimeout(10, 1, TimeUnit.SECONDS, scheduler, new CheckedConsumer<Integer>() {
                    @Override
                    public void accept(Integer e) throws Exception {
                        evicted.add(e);
                        ts.cancel();
                    }
                })
                .subscribe(ts);

        TestHelper.emit(pp, 1, 2, 3, 4, 5);

        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), evicted);
    }

    @Test
    public void evictThrows() {
        TestHelper.withErrorTracking(errors -> {
            TestSchedulerService scheduler = new TestSchedulerService();

            DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = new TestConsumer<Integer>(0L);

            pp
                    .onBackpressureTimeout(10, 1, TimeUnit.SECONDS, scheduler, new CheckedConsumer<Integer>() {
                        @Override
                        public void accept(Integer e) throws Exception {
                            throw new IOException(e.toString());
                        }
                    })
                    .subscribe(ts);

            TestHelper.emit(pp, 1, 2, 3, 4, 5);

            scheduler.advanceTimeBy(1, TimeUnit.MINUTES);

            ts.requestMore(1).assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "1");
            TestHelper.assertError(errors, 1, IOException.class, "2");
            TestHelper.assertError(errors, 2, IOException.class, "3");
            TestHelper.assertError(errors, 3, IOException.class, "4");
            TestHelper.assertError(errors, 4, IOException.class, "5");
        });
    }

    @Test
    public void cancelAndRequest() {
        Folyam.range(1, 5)
                .onBackpressureTimeout(1, TimeUnit.MINUTES, SchedulerServices.single(), this)
                .subscribe(new TestConsumer<Integer>(1) {
                    @Override
                    public void onNext(Integer t) {
                        super.onNext(t);
                        cancel();
                    }
                });
    }

    @Test
    public void example() {
        Folyam.intervalRange(1, 5, 100, 100, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                .onBackpressureTimeout(2, 100, TimeUnit.MILLISECONDS,
                                SchedulerServices.single(), this)
                .test(0)
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult();

        assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L), evicted);
    }

}
