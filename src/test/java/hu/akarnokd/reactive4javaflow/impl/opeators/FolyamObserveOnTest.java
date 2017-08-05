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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.hot.SolocastProcessor;
import hu.akarnokd.reactive4javaflow.impl.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class FolyamObserveOnTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .observeOn(SchedulerServices.single()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .observeOn(SchedulerServices.single()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .observeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorHidden() {
        Folyam.error(new IOException())
                .hide()
                .observeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .observeOn(SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorHiddenConditional() {
        Folyam.error(new IOException())
                .hide()
                .observeOn(SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }

    @Test
    public void longStream() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 1000)
                    .observeOn(SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void longStreamConditional() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 1000)
                    .observeOn(SchedulerServices.single())
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void longStreamHidden() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 1000)
                    .hide()
                    .observeOn(SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void longStreamConditionalHidden() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 1000)
                    .hide()
                    .observeOn(SchedulerServices.single())
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void prefetch() {
        for (int i = 1; i <= 128; i *= 2) {
            Folyam.range(1, 1000)
                    .observeOn(SchedulerServices.single(), i)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void prefetchConditional() {
        for (int i = 1; i <= 128; i *= 2) {
            Folyam.range(1, 1000)
                    .observeOn(SchedulerServices.single(), i)
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void prefetchHide() {
        for (int i = 1; i <= 128; i *= 2) {
            Folyam.range(1, 1000)
                    .hide()
                    .observeOn(SchedulerServices.single(), i)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void prefetchConditionalHide() {
        for (int i = 1; i <= 128; i *= 2) {
            Folyam.range(1, 1000)
                    .hide()
                    .observeOn(SchedulerServices.single(), i)
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(1000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void overflowAndError() {
        TestHelper.withErrorTracking(errors -> {
            BooleanSubscription bs = new BooleanSubscription();
            new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(bs);
                    s.onNext(1);
                    s.onNext(2);
                    s.onError(new IOException());
                }
            }
            .observeOn(SchedulerServices.trampoline(), 1)
            .test(0, false, FusedSubscription.SYNC)
            .assertFusionMode(FusedSubscription.NONE)
            .assertEmpty()
            .requestMore(1)
            .assertFailure(IllegalStateException.class, 1);

            assertTrue(bs.isCancelled());

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void fusedRequestMore() {
        Folyam.range(1, 1000)
                .observeOn(SchedulerServices.single())
                .observeOn(SchedulerServices.trampoline())
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .awaitDone(5, TimeUnit.SECONDS)
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void asyncErrorFused() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        TestConsumer<Integer> tc = sp.observeOn(SchedulerServices.trampoline())
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        sp.onNext(1);
        sp.onError(new IOException());

        tc.assertFailure(IOException.class, 1);
    }


    @Test
    public void asyncErrorFusedConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        TestConsumer<Integer> tc = sp.observeOn(SchedulerServices.trampoline())
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        sp.onNext(1);
        sp.onError(new IOException());

        tc.assertFailure(IOException.class, 1);
    }

    @Test
    public void errorBackpressured() {
        Folyam.error(new IOException())
                .observeOn(SchedulerServices.trampoline())
                .test(0)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorBackpressuredConditional() {
        Folyam.error(new IOException())
                .observeOn(SchedulerServices.trampoline())
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void asyncFusedSourceNormalDrain() {
        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.ASYNC));
                s.onNext(null);
            }
        }
        .observeOn(SchedulerServices.trampoline())
        .test()
        .assertFailureAndMessage(IOException.class, "Forced failure");
    }


    @Test
    public void asyncFusedSourceNormalDrainConditional() {
        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.ASYNC));
                s.onNext(null);
            }
        }
        .observeOn(SchedulerServices.trampoline())
        .filter(v -> true)
        .test()
        .assertFailureAndMessage(IOException.class, "Forced failure");
    }

    @Test
    public void syncFusedSourceNormalDrain() {
        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                s.onNext(null);
            }
        }
                .observeOn(SchedulerServices.trampoline())
                .test()
                .assertFailureAndMessage(IOException.class, "Forced failure");
    }


    @Test
    public void syncFusedSourceNormalDrainConditional() {
        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                s.onNext(null);
            }
        }
                .observeOn(SchedulerServices.trampoline())
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IOException.class, "Forced failure");
    }

}
