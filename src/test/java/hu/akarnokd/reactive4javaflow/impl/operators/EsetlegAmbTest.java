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
import java.util.*;

import static org.junit.Assert.*;

public class EsetlegAmbTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.ambArray(Esetleg.just(1), Esetleg.never()),
                1
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Esetleg.ambArray(Esetleg.never(), Esetleg.just(1), Esetleg.never()),
                1
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.ambArray(Esetleg.never(), Esetleg.empty(), Esetleg.never())
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Esetleg.ambArray(Esetleg.just(1))
                , 1
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Esetleg.ambArray()
        );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Esetleg.amb(List.of(Esetleg.just(1), Esetleg.never())),
                1
        );
    }

    @Test
    public void error() {
        Esetleg.ambArray(Esetleg.error(new IOException()), Esetleg.just(1))
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
    @SuppressWarnings("unchecked")
    public void singleNull() {
        Esetleg.ambArray(new Esetleg[] { null })
        .test()
        .assertFailure(NullPointerException.class);
    }

    @Test
    public void firstNull() {
        Esetleg.ambArray(null, Esetleg.just(1))
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void secondNull() {
        Esetleg.ambArray(Esetleg.never(), null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void firstNullConditional() {
        Esetleg.ambArray(null, Esetleg.just(1))
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void secondNullConditional() {
        Esetleg.ambArray(Esetleg.never(), null)
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void cancel() {
        Esetleg.ambArray(Esetleg.never(), Esetleg.just(1))
                .test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void conditional() {
        Esetleg.ambArray(Esetleg.never(), Esetleg.just(1))
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
        TestConsumer<Integer> tc = dp1.ambWith(Esetleg.never())
        .test();

        dp1.onNext(1);
        dp1.onError(new IOException());

        tc.assertFailure(IOException.class, 1);
    }


    @Test
    public void itemAndErrorConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        TestConsumer<Integer> tc = dp1.ambWith(Esetleg.never())
                .filter(v -> true)
                .test();

        dp1.onNext(1);
        dp1.onNext(2);
        dp1.onError(new IOException());

        tc.assertFailure(IOException.class, 1, 2);
    }

    @Test
    public void ambWithMany() {
        Esetleg<Integer> f = Esetleg.never();
        for (int i = 0; i < 1000; i++) {
            f = f.ambWith(Esetleg.never());
        }
        f = f.ambWith(Esetleg.just(1));

        f.test().assertResult(1);

        f.filter(v -> true).test().assertResult(1);
    }

    @Test
    public void ambIterableMany() {
        List<Esetleg<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Esetleg.never());
        }
        list.add(Esetleg.just(1));

        Esetleg.amb(list)
                .test()
                .assertResult(1);
    }

    @Test
    public void ambIterableEmpty() {
        Esetleg.amb(List.of())
                .test()
                .assertResult();
    }

    @Test
    public void ambIterableOne() {
        Esetleg.amb(List.of(Esetleg.just(1)))
                .test()
                .assertResult(1);
    }

    @Test
    public void ambIterableOneNull() {
        Esetleg.amb(Collections.singleton(null))
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void iterableFails() {
        Esetleg.amb(new FailingMappedIterable<>(1, 10, 10, idx -> Esetleg.never()))
                .test()
                .assertFailure(IllegalStateException.class);
    }
}
