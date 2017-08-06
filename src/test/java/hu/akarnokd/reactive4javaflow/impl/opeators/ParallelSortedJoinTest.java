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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

public class ParallelSortedJoinTest {

    @Test
    public void cancel() {
        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
        .parallel()
        .sorted(Comparator.naturalOrder())
        .test();

        assertTrue(pp.hasSubscribers());

        ts.cancel();

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void error() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>error(new IOException())
                    .parallel()
                    .sorted(Comparator.naturalOrder())
                    .test()
                    .assertFailure(IOException.class);

            assertTrue(errors.isEmpty());
        });
    }

    @Test
    public void error3() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>error(new IOException())
                    .parallel()
                    .sorted(Comparator.naturalOrder())
                    .test(0)
                    .assertFailure(IOException.class);

            assertTrue(errors.isEmpty());
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void error2() {
        TestHelper.withErrorTracking(errors -> {
            ParallelFolyam.fromArray(Folyam.<Integer>error(new IOException()), Folyam.<Integer>error(new IOException()))
                    .sorted(Comparator.naturalOrder())
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void comparerCrash() {
        Folyam.fromArray(4, 3, 2, 1)
        .parallel(2)
        .sorted(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if (o1 == 4 && o2 == 3) {
                    throw new IllegalArgumentException();
                }
                return o1.compareTo(o2);
            }
        })
        .test()
        .assertFailure(IllegalArgumentException.class, 1, 2);
    }

    @Test
    public void empty() {
        Folyam.<Integer>empty()
        .parallel()
        .sorted(Comparator.naturalOrder())
        .test()
        .assertResult();
    }

    @Test
    public void asyncDrain() {
        Integer[] values = new Integer[100 * 1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = values.length - i;
        }

        TestConsumer<Integer> ts = Folyam.fromArray(values)
        .parallel(2)
        .runOn(SchedulerServices.computation())
        .sorted(Comparator.naturalOrder())
        .observeOn(SchedulerServices.single())
        .test();

        ts
        .awaitDone(5, TimeUnit.SECONDS)
        .assertValueCount(values.length)
        .assertNoErrors()
        .assertComplete();

        List<Integer> list = ts.values();
        for (int i = 0; i < values.length; i++) {
            assertEquals(i + 1, list.get(i).intValue());
        }
    }

    @Test
    public void sortCancelRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> pp = new DirectProcessor<>();
            pp.onNext(1);
            pp.onNext(2);

            final TestConsumer<Integer> ts = pp.parallel(2)
            .sorted(Comparator.naturalOrder())
            .test();

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onComplete();
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void sortCancelRace2() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> pp = new DirectProcessor<>();
            pp.onNext(1);
            pp.onNext(2);

            final TestConsumer<Integer> ts = pp.parallel(2)
            .sorted(Comparator.naturalOrder())
            .test(0);

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onComplete();
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }
}
