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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FolyamSwitchMapTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.just(Folyam.range(1, 5)).hide()
                .switchMap(v -> v)
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.just(Folyam.range(1, 5)).hide()
                        .switchMap(v -> v, 1)
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardDelayError() {
        TestHelper.assertResult(
                Folyam.just(Folyam.range(1, 5)).hide()
                        .switchMapDelayError(v -> v)
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard1DelayError() {
        TestHelper.assertResult(
                Folyam.just(Folyam.range(1, 5)).hide()
                        .switchMapDelayError(v -> v, 1)
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void checkSwitch() {
        DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                .test();

        dp.onNext(dp1);

        assertTrue(dp1.hasSubscribers());

        dp1.onNext(1);

        tc.assertValues(1);

        dp1.onNext(2);

        tc.assertValues(1, 2);

        dp.onNext(dp2);

        assertFalse(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onNext(3);

        tc.assertValues(1, 2, 3);

        dp.onComplete();

        tc.assertNotComplete();

        dp2.onNext(4);

        tc.assertValues(1, 2, 3, 4);

        dp2.onComplete();

        tc.assertResult(1, 2, 3, 4);
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.switchMap(v -> Folyam.just(v + 1)), IOException.class);
    }

    @Test
    public void errorDelayed() {
        TestHelper.assertFailureComposed(-1,
                f -> f.switchMapDelayError(v -> Folyam.just(v + 1)), IOException.class);
    }

    @Test
    public void errorInner() {
        TestHelper.assertFailureComposed(5,
                f -> f.switchMap(v -> {
                    if (v == 3) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v + 1);
                }), IOException.class, 2, 3);
    }

    @Test
    public void errorInnerDelayError() {
        TestHelper.assertFailureComposed(5,
                f -> f.switchMapDelayError(v -> {
                    if (v == 3) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v + 1);
                }), IOException.class, 2, 3, 5, 6);
    }

    @Test
    public void mapperCrash() {
        TestHelper.assertFailureComposed(5,
                f -> f.switchMap(v -> {
                    if (v == 3) {
                        throw new IOException();
                    }
                    return Folyam.just(v + 1);
                }), IOException.class, 2, 3);
    }

    @Test
    public void mapperCrashDelayError() {
        TestHelper.assertFailureComposed(5,
                f -> f.switchMapDelayError(v -> {
                    if (v == 3) {
                        throw new IOException();
                    }
                    return Folyam.just(v + 1);
                }), IOException.class, 2, 3);
    }

    @Test
    public void empty() {
        Folyam.empty()
                .switchMap(Folyam::just)
                .test()
                .assertResult();
    }

    @Test
    public void conditional() {
        Folyam.empty()
                .switchMap(Folyam::just)
                .test()
                .assertResult();
    }

    @Test
    public void mixed() {
        Folyam.range(1, 10)
                .switchMap(v -> v % 2 == 0 ? Folyam.just(v) : Folyam.empty())
                .test()
                .assertResult(2, 4, 6, 8, 10);
    }

    @Test
    public void request0Upfront() {
        Folyam.fromArray(Folyam.range(1, 5), Folyam.range(6, 5))
                .switchMap(v -> v)
                .test(0)
                .assertEmpty()
                .requestMore(Long.MAX_VALUE)
                .assertResult(6, 7, 8, 9, 10);
    }


    @Test
    public void request0UpfrontConditional() {
        Folyam.fromArray(Folyam.range(1, 5), Folyam.range(6, 5))
                .switchMap(v -> v)
                .filter(v -> true)
                .test(0)
                .assertEmpty()
                .requestMore(Long.MAX_VALUE)
                .assertResult(6, 7, 8, 9, 10);
    }

    @Test
    public void normal() {
        Folyam.fromArray(Folyam.range(1, 5), Folyam.range(6, 5))
                .switchMap(v -> v)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }


    @Test
    public void normalHidden() {
        Folyam.fromArray(Folyam.range(1, 5).hide(), Folyam.range(6, 5).hide())
                .switchMap(v -> v)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }


    @Test
    public void normalConditional() {
        Folyam.fromArray(Folyam.range(1, 5), Folyam.range(6, 5))
                .switchMap(v -> v)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }


    @Test
    public void normalConditionalHidden() {
        Folyam.fromArray(Folyam.range(1, 5).hide(), Folyam.range(6, 5).hide())
                .switchMap(v -> v)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void innerFusedCrash() {
        Folyam.just(new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        })
                .switchMap(v -> v)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerFusedCrashConditional() {
        Folyam.just(new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        })
                .switchMap(v -> v)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void innerFusedCrashDelayError() {
        Folyam.just(new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        })
                .switchMapDelayError(v -> v)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerFusedCrashDelayErrorConditional() {
        Folyam.just(new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        })
                .switchMapDelayError(v -> v)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void asyncSwitching() {
        Folyam.range(1, 1000).subscribeOn(SchedulerServices.computation())
        .switchMap(v -> Folyam.range(1, 100).subscribeOn(SchedulerServices.computation()))
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void nextSourceCancelRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .test();

            dp.onNext(dp1);

            Runnable r1 = tc::cancel;

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }


    @Test
    public void nextSourceCancelRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .filter(v -> true)
                    .test();

            dp.onNext(dp1);

            Runnable r1 = tc::cancel;

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void nextSourceNextInnerRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .test();

            dp.onNext(dp1);

            Runnable r1 = () -> dp1.onNext(1);

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }


    @Test
    public void nextSourceNextInnerRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .filter(v -> true)
                    .test();

            dp.onNext(dp1);

            Runnable r1 = () -> dp1.onNext(1);

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }


    @Test
    public void nextSourceCancelRaceBackpressured() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .test(0L);

            dp.onNext(dp1);

            Runnable r1 = tc::cancel;

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }


    @Test
    public void nextSourceCancelRaceConditionalBackpressured() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .filter(v -> true)
                    .test(0L);

            dp.onNext(dp1);

            Runnable r1 = tc::cancel;

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void nextSourceNextInnerRaceBackpressured() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .test(0L);

            dp.onNext(dp1);

            Runnable r1 = () -> dp1.onNext(1);

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }


    @Test
    public void nextSourceNextInnerRaceConditionalBackpressured() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Folyam<Integer>> dp = new DirectProcessor<>();

            DirectProcessor<Integer> dp1 = new DirectProcessor<>();
            DirectProcessor<Integer> dp2 = new DirectProcessor<>();

            TestConsumer<Integer> tc = dp.switchMap(v -> v, 1)
                    .filter(v -> true)
                    .test(0L);

            dp.onNext(dp1);

            Runnable r1 = () -> dp1.onNext(1);

            Runnable r2 = () -> dp.onNext(dp2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void errorInnerBackpressured() {
        Folyam.switchNext(Folyam.just(Folyam.error(new IOException()).hide()).hide())
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressuredConditional() {
        Folyam.switchNext(Folyam.just(Folyam.error(new IOException()).hide()).hide(), 1)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressuredDelayError() {
        Folyam.switchNextDelayError(Folyam.just(Folyam.error(new IOException()).hide()).hide())
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressuredConditionalDelayError() {
        Folyam.switchNextDelayError(Folyam.just(Folyam.error(new IOException()).hide()).hide(), 1)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void mainErrorBackpressured() {
        Folyam.error(new IOException())
                .switchMap(Folyam::just)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void mainErrorBackpressuredConditional() {
        Folyam.error(new IOException())
                .switchMap(Folyam::just)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }
}
