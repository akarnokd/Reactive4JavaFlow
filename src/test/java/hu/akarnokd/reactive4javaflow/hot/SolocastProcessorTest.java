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

package hu.akarnokd.reactive4javaflow.hot;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class SolocastProcessorTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.defer(() -> {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();
            Folyam.range(1, 5).subscribe(sp);
            return sp;
        }), 1, 2, 3, 4, 5);
    }


    @Test
    public void offlineNormal() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        assertFalse(sp.hasSubscribers());
        assertFalse(sp.hasComplete());
        assertFalse(sp.hasThrowable());
        assertNull(sp.getThrowable());

        for (int i = 0; i < 5; i++) {
            sp.onNext(i);
        }
        sp.onComplete();

        sp.test()
        .assertResult(0, 1, 2, 3, 4);

        assertFalse(sp.hasSubscribers());
        assertTrue(sp.hasComplete());
        assertFalse(sp.hasThrowable());
        assertNull(sp.getThrowable());
    }


    @Test
    public void offlineNormal2() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>(8);

        Folyam.range(0, 5).subscribe(sp);

        sp.test()
                .assertResult(0, 1, 2, 3, 4);
    }

    @Test
    public void offlineError() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onError(new IOException());

        sp.test().assertFailure(IOException.class, 1);

        sp.test().assertFailure(IllegalStateException.class);

        assertFalse(sp.hasSubscribers());
        assertFalse(sp.hasComplete());
        assertTrue(sp.hasThrowable());
        assertTrue("" + sp.getThrowable(), sp.getThrowable() instanceof IOException);
    }

    @Test
    public void onlineNormal() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        assertFalse(sp.hasSubscribers());

        TestConsumer<Integer> tc = sp.test();

        assertTrue(sp.hasSubscribers());

        for (int i = 0; i < 5; i++) {
            sp.onNext(i);
        }
        sp.onComplete();

        assertFalse(sp.hasSubscribers());

        tc.assertResult(0, 1, 2, 3, 4);
    }

    @Test
    public void onlineError() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        TestConsumer<Integer> tc = sp.test();

        for (int i = 0; i < 5; i++) {
            sp.onNext(i);
        }
        sp.onError(new IOException());

        tc.assertFailure(IOException.class, 0, 1, 2, 3, 4);
    }

    @Test
    public void cancelImmediately() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        sp.test(0, true, 0).assertEmpty();

        assertFalse(sp.hasSubscribers());
    }

    @Test(expected = NullPointerException.class)
    public void nullItem() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(null);
    }


    @Test(expected = NullPointerException.class)
    public void nullError() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onError(null);
    }

    @Test
    public void onErrorMulti() {
        TestHelper.withErrorTracking(errors -> {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();
            sp.onError(new IOException());

            sp.test().assertFailure(IOException.class);

            sp.onError(new IOException());

            TestHelper.assertError(errors,0, IOException.class);
        });
    }

    @Test
    public void cancelSource() {
        LastProcessor<Integer> sp0 = new LastProcessor<>();
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onComplete();

        sp0.subscribe(sp);

        assertFalse(sp0.hasSubscribers());
    }

    @Test
    public void errorBackpressured() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onError(new IOException());

        sp.test(0).assertFailure(IOException.class);
    }

    @Test
    public void errorFusedBackpressured() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onError(new IOException());

        sp.test(0, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.ASYNC)
                .assertFailure(IOException.class);
    }

    @Test
    public void emissionSubscriptionRace() {
        for (int i = 0; i < 1000; i++) {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();

            TestConsumer<Integer> tc = new TestConsumer<>();

            Runnable r1 = () -> {
                for (int j = 0; j < 100; j++) {
                    sp.onNext(j);
                }
                sp.onComplete();
            };

            Runnable r2 = () -> sp.subscribe(tc);

            TestHelper.race(r1, r2);

            for (int j = 0; j < 100; j++) {
                tc.assertValueAt(j, j);
            }
            tc.assertComplete()
                    .assertNoErrors();
        }
    }

    @Test
    public void unsupportedFusionMode() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        sp.onNext(1);
        sp.onComplete();

        sp.test(1, false, FusedSubscription.SYNC)
                .assertFusionMode(FusedSubscription.NONE)
                .assertResult(1);
    }

    @Test
    public void emissionSubscriptionRaceFused() {
        for (int i = 0; i < 1000; i++) {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();

            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.requestFusionMode(FusedSubscription.ANY);

            Runnable r1 = () -> {
                for (int j = 0; j < 100; j++) {
                    sp.onNext(j);
                }
                sp.onComplete();
            };

            Runnable r2 = () -> sp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertFusionMode(FusedSubscription.ASYNC);

            tc.awaitDone(5, TimeUnit.SECONDS);

            for (int j = 0; j < 100; j++) {
                tc.assertValueAt(j, j);
            }
            tc.assertComplete()
                    .assertNoErrors();
        }
    }

    @Test
    public void emissionSubscriptionRaceCancelled() {
        for (int i = 0; i < 1000; i++) {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();

            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.cancel();

            Runnable r1 = () -> {
                for (int j = 0; j < 100; j++) {
                    sp.onNext(j);
                }
                sp.onComplete();
            };

            Runnable r2 = () -> sp.subscribe(tc);

            TestHelper.race(r1, r2);

            tc.assertEmpty();
        }
    }

    @Test
    public void fusedEmissionCancelRace() {
        for (int i = 0; i < 1000; i++) {
            SolocastProcessor<Integer> sp = new SolocastProcessor<>();

            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.requestFusionMode(FusedSubscription.ANY);
            sp.subscribe(tc);

            Runnable r1 = () -> {
                for (int j = 0; j < 100; j++) {
                    sp.onNext(j);
                }
                sp.onComplete();
            };

            Runnable r2 = tc::cancel;

            TestHelper.race(r1, r2);
        }
    }

}
