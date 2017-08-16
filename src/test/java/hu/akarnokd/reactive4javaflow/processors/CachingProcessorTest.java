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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class CachingProcessorTest {

    @Test
    public void standard() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();
        Folyam.range(1, 5).subscribe(cp);
        TestHelper.assertResult(cp, 1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();
        cp.onError(new IOException());

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertTrue(cp.hasThrowable());
        assertTrue(cp.getThrowable() + "", cp.getThrowable() instanceof IOException);
        assertFalse(cp.hasValues());

        TestHelper.assertFailureComposed(0, f -> cp, IOException.class);
    }

    @Test
    public void manyItems() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());

        TestConsumer<Integer> tc = cp.test();

        for (int i = 0; i < 1000; i++) {
            cp.onNext(i);
        }
        cp.onComplete();

        tc.assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();

        cp.test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void manyItems2() {
        CachingProcessor<Integer> cp = CachingProcessor.withCapacityHint(FolyamPlugins.defaultBufferSize());

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());

        TestConsumer<Integer> tc = cp.test();

        assertTrue(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());
        assertFalse(cp.hasValues());

        for (int i = 0; i < 1000; i++) {
            cp.onNext(i);
        }
        cp.onComplete();

        assertFalse(cp.hasSubscribers());
        assertTrue(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());
        assertTrue(cp.hasValues());

        tc.assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();

        cp.test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();

        Integer[] ints = cp.getValues(new Integer[0]);
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, ints[i].intValue());
        }

        ints = cp.getValues(new Integer[1001]);
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, ints[i].intValue());
        }
        assertNull(ints[1000]);
    }

    @Test
    public void standardSizeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(3);
        Folyam.range(1, 5).subscribe(cp);
        TestHelper.assertResult(cp, 3, 4, 5);
    }

    @Test
    public void errorSizeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(10);
        cp.onError(new IOException());

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertTrue(cp.hasThrowable());
        assertTrue(cp.getThrowable() + "", cp.getThrowable() instanceof IOException);
        assertFalse(cp.hasValues());

        TestHelper.assertFailureComposed(0, f -> cp, IOException.class);
    }

    @Test
    public void manyItemsSizeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(10);

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());

        TestConsumer<Integer> tc = cp.test();

        for (int i = 0; i < 1000; i++) {
            cp.onNext(i);
        }
        cp.onComplete();

        tc.assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();

        cp.test()
                .assertValueCount(10)
                .assertNoErrors()
                .assertComplete();

        Integer[] ints = cp.getValues(new Integer[0]);
        int j = 0;
        for (int i = 990; i < 1000; i++) {
            assertEquals(i , ints[j++].intValue());
        }

        ints = cp.getValues(new Integer[11]);
        j = 0;
        for (int i = 990; i < 1000; i++) {
            assertEquals(i , ints[j++].intValue());
        }
        assertNull(ints[10]);
    }


    @Test
    public void standardSizeAndTimeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(3, 1, TimeUnit.DAYS, SchedulerServices.single());
        Folyam.range(1, 5).subscribe(cp);
        TestHelper.assertResult(cp, 3, 4, 5);
    }

    @Test
    public void errorSizeAndTimeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(10, 1, TimeUnit.DAYS, SchedulerServices.single());
        cp.onError(new IOException());

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertTrue(cp.hasThrowable());
        assertTrue(cp.getThrowable() + "", cp.getThrowable() instanceof IOException);
        assertFalse(cp.hasValues());

        TestHelper.assertFailureComposed(0, f -> cp, IOException.class);
    }

    @Test
    public void manyItemsSizeAndTimeBound() {
        CachingProcessor<Integer> cp = new CachingProcessor<>(10, 1, TimeUnit.DAYS, SchedulerServices.single());

        assertFalse(cp.hasSubscribers());
        assertFalse(cp.hasComplete());
        assertFalse(cp.hasThrowable());
        assertNull(cp.getThrowable());

        TestConsumer<Integer> tc = cp.test();

        for (int i = 0; i < 1000; i++) {
            cp.onNext(i);
        }
        cp.onComplete();

        tc.assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();

        cp.test()
                .assertValueCount(10)
                .assertNoErrors()
                .assertComplete();

        Integer[] ints = cp.getValues(new Integer[0]);
        int j = 0;
        for (int i = 990; i < 1000; i++) {
            assertEquals(i , ints[j++].intValue());
        }

        ints = cp.getValues(new Integer[11]);
        j = 0;
        for (int i = 990; i < 1000; i++) {
            assertEquals(i , ints[j++].intValue());
        }
        assertNull(ints[10]);
    }

    @Test
    public void timebound() {
        TestSchedulerService executor = new TestSchedulerService();

        CachingProcessor<Integer> cp = new CachingProcessor<>(10, TimeUnit.MILLISECONDS, executor);

        cp.onNext(1);

        assertArrayEquals(new Integer[] { 1 }, cp.getValues(new Integer[0]));

        cp.onNext(2);

        assertArrayEquals(new Integer[] { 1, 2 }, cp.getValues(new Integer[0]));

        executor.advanceTimeBy(5, TimeUnit.MILLISECONDS);

        cp.onNext(3);

        assertArrayEquals(new Integer[] { 1, 2, 3 }, cp.getValues(new Integer[0]));

        executor.advanceTimeBy(5, TimeUnit.MILLISECONDS);

        assertArrayEquals(new Integer[] { 3 }, cp.getValues(new Integer[0]));

        executor.advanceTimeBy(5, TimeUnit.MILLISECONDS);

        assertArrayEquals(new Integer[] { }, cp.getValues(new Integer[0]));

        cp.onNext(4);
        cp.onComplete();

        cp.test().assertResult(4);

        assertArrayEquals(new Integer[] { 4 }, cp.getValues(new Integer[0]));

        assertArrayEquals(new Integer[] { 4, null }, cp.getValues(new Integer[2]));

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        cp.test().assertResult();

        assertArrayEquals(new Integer[] { }, cp.getValues(new Integer[0]));
    }

    @Test
    public void subscribersComeAndGo() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();

        TestConsumer<Integer> tc1 = cp.test();
        TestConsumer<Integer> tc2 = cp.test();
        TestConsumer<Integer> tc3 = cp.test();

        tc2.cancel();

        cp.onComplete();

        tc1.assertResult();
        tc3.assertResult();
    }


    @Test
    public void subscribersComeAndGo2() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();

        TestConsumer<Integer> tc1 = cp.test();
        TestConsumer<Integer> tc2 = cp.test(0, true, 0);

        tc1.cancel();

        cp.onComplete();

        tc1.assertEmpty();
        tc2.assertEmpty();
    }

    @Test
    public void prepareAndCancel() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();

        cp.prepare(new BooleanSubscription());

        TestConsumer<Integer> tc1 = cp.test();

        cp.close();

        tc1.assertFailure(CancellationException.class);

        cp.test().assertFailure(CancellationException.class);

        TestHelper.withErrorTracking(errors -> {
            cp.onError(new IOException());

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void fusedLongPoll() {
        CachingProcessor<Integer>[] cps = new CachingProcessor[] {
                new CachingProcessor<>(),
                new CachingProcessor<>(10),
                new CachingProcessor<>(1, TimeUnit.DAYS, SchedulerServices.single())
        };

        for (int j = 0; j < 3; j++) {
            CachingProcessor<Integer> cp = cps[j];

            TestConsumer<Integer> tc = cp.test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                    .withTag("" + j);

            for (int i = 0; i < 1000; i++) {
                cp.onNext(i);
            }
            cp.onComplete();

            tc.assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void onNextSubscribeRaceUnbounded() {
        for (int i = 0; i < 100000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();

            CachingProcessor<Integer> cp = new CachingProcessor<>();

            Runnable r1 = () -> cp.onNext(1);

            Runnable r2 = () -> cp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertValues(1);
        }
    }


    @Test
    public void onCompleteSubscribeRaceUnbounded() {
        for (int i = 0; i < 100000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();

            CachingProcessor<Integer> cp = new CachingProcessor<>();

            Runnable r1 = cp::onComplete;

            Runnable r2 = () -> cp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertResult();
        }
    }

    @Test
    public void onNextSubscribeRaceSizeBound() {
        for (int i = 0; i < 100000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();

            CachingProcessor<Integer> cp = new CachingProcessor<>(10);

            Runnable r1 = () -> cp.onNext(1);

            Runnable r2 = () -> cp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertValues(1);
        }
    }

    @Test
    public void onNextSubscribeRaceSizeBound2() {
        for (int i = 0; i < 100000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();

            CachingProcessor<Integer> cp = new CachingProcessor<>(10);
            cp.onNext(0);

            Runnable r1 = () -> cp.onNext(1);

            Runnable r2 = () -> cp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertValues(0, 1);
        }
    }

    @Test
    public void onNextSubscribeRaceTimeBound() {
        for (int i = 0; i < 100000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();

            CachingProcessor<Integer> cp = new CachingProcessor<>(10, TimeUnit.SECONDS, SchedulerServices.single());

            Runnable r1 = () -> cp.onNext(1);

            Runnable r2 = () -> cp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertValues(1);
        }
    }
}
