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

import static org.junit.Assert.assertFalse;

public class FolyamValveTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 10).valve(Folyam.never())
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 10).valve(Folyam.never(), 2)
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void passthrough() {
        Folyam.range(1, 10)
                .valve(Folyam.never())
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void gatedoff() {
        Folyam.range(1, 10)
                .valve(Folyam.never(), false)
                .test()
                .assertEmpty();
    }

    @Test
    public void syncGating() {
        DirectProcessor<Boolean> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = Folyam.range(1, 10)
                .valve(pp, false)
                .test();

        ts.assertEmpty();

        pp.onNext(true);

        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void syncGatingBackpressured() {
        DirectProcessor<Boolean> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = Folyam.range(1, 10)
                .valve(pp, false)
                .test(5);

        ts.assertEmpty();

        pp.onNext(true);

        ts.assertValues(1, 2, 3, 4, 5).assertNotComplete().assertNoErrors();

        pp.onNext(false);

        ts.requestMore(5);

        ts.assertValues(1, 2, 3, 4, 5).assertNotComplete().assertNoErrors();

        pp.onNext(true);

        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void gating() {
        Folyam.intervalRange(1, 10, 17, 17, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .valve(
                        Folyam.interval(50, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                                .map(v -> (v & 1) == 0), 16, true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    @Test
    public void mainError() {
        Folyam.<Integer>error(new IOException())
                .valve(Folyam.never())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void otherError() {
        Folyam.just(1)
                .valve(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void otherCompletes() {
        Folyam.just(1)
                .valve(Folyam.empty())
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void bothError() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>error(new IllegalArgumentException())
                    .valve(Folyam.error(new IOException()))
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void take() {
        Folyam.range(1, 10)
                .valve(Folyam.never())
                .take(5)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void openCloseRace() {
        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp1 = new DirectProcessor<>();
            final DirectProcessor<Boolean> pp2 = new DirectProcessor<>();

            TestConsumer<Integer> ts = pp1.valve(pp2, false)
                    .test();

            Runnable r1 = () -> pp1.onNext(1);

            Runnable r2 = () -> pp2.onNext(true);

            TestHelper.race(r1, r2);

            ts.assertValues(1).assertNoErrors().assertNotComplete();
        }
    }
}
