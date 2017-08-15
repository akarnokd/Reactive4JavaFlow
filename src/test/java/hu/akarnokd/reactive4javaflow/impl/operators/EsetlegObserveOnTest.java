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
import hu.akarnokd.reactive4javaflow.hot.SolocastProcessor;
import hu.akarnokd.reactive4javaflow.impl.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class EsetlegObserveOnTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .observeOn(SchedulerServices.single()),
                1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .observeOn(SchedulerServices.single()),
                1);
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .observeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorHidden() {
        Esetleg.error(new IOException())
                .hide()
                .observeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Esetleg.error(new IOException())
                .observeOn(SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorHiddenConditional() {
        Esetleg.error(new IOException())
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
            Esetleg.just(1)
                    .observeOn(SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1);
        }
    }


    @Test
    public void longStreamConditional() {
        for (int i = 0; i < 100; i++) {
            Esetleg.just(1)
                    .observeOn(SchedulerServices.single())
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1);
        }
    }


    @Test
    public void longStreamHidden() {
        for (int i = 0; i < 100; i++) {
            Esetleg.just(1)
                    .hide()
                    .observeOn(SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1);
        }
    }


    @Test
    public void longStreamConditionalHidden() {
        for (int i = 0; i < 100; i++) {
            Esetleg.just(1)
                    .hide()
                    .observeOn(SchedulerServices.single())
                    .filter(v -> true)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1);
        }
    }

    @Test
    public void overflowAndError() {
        TestHelper.withErrorTracking(errors -> {
            BooleanSubscription bs = new BooleanSubscription();
            new Esetleg<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(bs);
                    s.onNext(1);
                    s.onNext(2);
                    s.onError(new IOException());
                }
            }
            .observeOn(SchedulerServices.trampoline())
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
        Esetleg.just(1)
                .observeOn(SchedulerServices.single())
                .observeOn(SchedulerServices.trampoline())
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1);
    }

    @Test
    public void errorBackpressured() {
        Esetleg.error(new IOException())
                .observeOn(SchedulerServices.trampoline())
                .test(0)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorBackpressuredConditional() {
        Esetleg.error(new IOException())
                .observeOn(SchedulerServices.trampoline())
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

}
