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
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Flow;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FolyamAmbTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.ambArray(Folyam.range(1, 5), Folyam.never()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.ambArray(Folyam.never(), Folyam.range(1, 5), Folyam.never()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.ambArray(Folyam.never(), Folyam.empty(), Folyam.never())
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.ambArray(Folyam.range(1, 5))
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.ambArray()
        );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Folyam.amb(List.of(Folyam.range(1, 5), Folyam.never())),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        Folyam.ambArray(Folyam.error(new IOException()), Folyam.range(1, 5))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void cancelsTheOthers1() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onNext(1);

        assertTrue(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

        dp1.onComplete();

        tc.assertResult(1);
    }

    @Test
    public void cancelsTheOthers2() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onNext(1);

        assertFalse(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onComplete();

        tc.assertResult(1);
    }

    @Test
    public void cancelsTheOthers3() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onComplete();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

        tc.assertResult();
    }

    @Test
    public void cancelsTheOthers4() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onComplete();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

        tc.assertResult();
    }

    @Test
    public void cancelsTheOthers5() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onError(new IOException());

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void cancelsTheOthers6() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onError(new IOException());

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void singleNull() {
        Folyam.ambArray(new Flow.Publisher[] { null })
        .test()
        .assertFailure(NullPointerException.class);
    }

    @Test
    public void firstNull() {
        Folyam.ambArray(null, Folyam.range(1, 5))
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void secondNull() {
        Folyam.ambArray(Folyam.never(), null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void firstNullConditional() {
        Folyam.ambArray(null, Folyam.range(1, 5))
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void secondNullConditional() {
        Folyam.ambArray(Folyam.never(), null)
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void cancel() {
        Folyam.ambArray(Folyam.never(), Folyam.range(1, 5))
                .test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void conditional() {
        Folyam.ambArray(Folyam.never(), Folyam.range(1, 5))
                .filter(v -> true)
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void winOnNextRace() {
        for (int i = 0; i < 1000; i++) {

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

            Runnable r1 = () -> dp1.onNext(1);
            Runnable r2 = () -> dp2.onNext(1);

            TestHelper.race(r1, r2);

            dp1.onComplete();
            dp2.onComplete();

            tc.assertResult(1);
        }
    }

    @Test
    public void winOnErrorRace() {
        for (int i = 0; i < 1000; i++) {
                TestHelper.withErrorTracking(errors -> {
                DirectProcessor<Integer> dp1 = new DirectProcessor<>();
                DirectProcessor<Integer> dp2 = new DirectProcessor<>();

                TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

                IOException ex = new IOException();

                Runnable r1 = () -> dp1.onError(ex);
                Runnable r2 = () -> dp2.onError(ex);

                TestHelper.race(r1, r2);

                tc.assertFailure(IOException.class);

                if (!errors.isEmpty()) {
                    TestHelper.assertError(errors, 0, IOException.class);
                }
            });
        }
    }

    @Test
    public void winOnCompleteRace() {
        for (int i = 0; i < 1000; i++) {

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp1.ambWith(dp2).test();

            Runnable r1 = dp1::onComplete;
            Runnable r2 = dp2::onComplete;

            TestHelper.race(r1, r2);

            tc.assertResult();
        }
    }

    @Test
    public void winOnNextRaceConditional() {
        for (int i = 0; i < 1000; i++) {

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp1.ambWith(dp2).filter(v -> true).test();

            Runnable r1 = () -> dp1.onNext(1);
            Runnable r2 = () -> dp2.onNext(1);

            TestHelper.race(r1, r2);

            dp1.onComplete();
            dp2.onComplete();

            tc.assertResult(1);
        }
    }

    @Test
    public void winOnErrorRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            TestHelper.withErrorTracking(errors -> {
                DirectProcessor<Integer> dp1 = new DirectProcessor<>();
                DirectProcessor<Integer> dp2 = new DirectProcessor<>();

                TestConsumer<Integer> tc = dp1.ambWith(dp2).filter(v -> true).test();

                IOException ex = new IOException();

                Runnable r1 = () -> dp1.onError(ex);
                Runnable r2 = () -> dp2.onError(ex);

                TestHelper.race(r1, r2);

                tc.assertFailure(IOException.class);

                if (!errors.isEmpty()) {
                    TestHelper.assertError(errors, 0, IOException.class);
                }
            });
        }
    }

    @Test
    public void winOnCompleteRaceConditional() {
        for (int i = 0; i < 1000; i++) {

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp1.ambWith(dp2).filter(v -> true).test();

            Runnable r1 = dp1::onComplete;
            Runnable r2 = dp2::onComplete;

            TestHelper.race(r1, r2);

            tc.assertResult();
        }
    }

    @Test
    public void itemAndError() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        TestConsumer<Integer> tc = dp1.ambWith(Folyam.never())
        .test();

        dp1.onNext(1);
        dp1.onError(new IOException());

        tc.assertFailure(IOException.class, 1);
    }


    @Test
    public void itemAndErrorConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        TestConsumer<Integer> tc = dp1.ambWith(Folyam.never())
                .filter(v -> true)
                .test();

        dp1.onNext(1);
        dp1.onNext(2);
        dp1.onError(new IOException());

        tc.assertFailure(IOException.class, 1, 2);
    }

    @Test
    public void ambWithMany() {
        Folyam<Integer> f = Folyam.never();
        for (int i = 0; i < 1000; i++) {
            f = f.ambWith(Folyam.never());
        }
        f = f.ambWith(Folyam.range(1, 5));

        f.test().assertResult(1, 2, 3, 4, 5);

        f.filter(v -> true).test().assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void ambIterableMany() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.never());
        }
        list.add(Folyam.range(1, 5));

        Folyam.amb(list)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void ambIterableEmpty() {
        Folyam.amb(List.of())
                .test()
                .assertResult();
    }

    @Test
    public void ambIterableOne() {
        Folyam.amb(List.of(Folyam.range(1, 5)))
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void ambIterableOneNull() {
        Folyam.amb(Collections.singleton(null))
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void iterableFails() {
        Folyam.amb(new FailingMappedIterable<>(1, 10, 10, idx -> Folyam.never()))
                .test()
                .assertFailure(IllegalStateException.class);
    }
}
