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
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Flow;

public class FolyamCallableTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.fromCallable(() -> 1), 1);
    }

    @Test
    public void error() {
        Folyam.fromCallable(() -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void nullReturn() {
        Folyam.fromCallable(() -> null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void cancelUpFront() {
        Folyam.fromCallable(() -> 1)
                .test(1, true, 0)
                .assertEmpty();
    }

    @Test
    public void wrongFusion() {
        Folyam.fromCallable(() -> 1)
                .test(1, false, FusedSubscription.SYNC)
                .assertFusionMode(FusedSubscription.NONE)
                .assertResult(1);
    }

    @Test
    public void fusedCrash() {
        Folyam.fromCallable(() -> 1)
                .map(v -> { throw new IOException(); })
                .test(1, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.ASYNC)
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedClear() {
        FusedSubscription[] fs = { null };
        Folyam.fromCallable(() -> 1)
                .subscribe(new FolyamSubscriber<Integer>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        fs[0] = (FusedSubscription)subscription;
                        fs[0].requestFusion(FusedSubscription.ANY);
                    }

                    @Override
                    public void onNext(Integer item) {

                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });
        fs[0].clear();
    }

    @Test
    public void requestRaceComplete() {
        for (int i = 0; i < 50000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>(0L);
            DeferredScalarSubscription<Integer> df = new DeferredScalarSubscription<>(tc);
            tc.onSubscribe(df);

            Runnable r1 = () -> df.request(1);
            Runnable r2 = () -> df.complete(1);

            TestHelper.race(r1, r2);

            tc.assertResult(1);
        }
    }


    @Test
    public void requestRequestRace() {
        for (int i = 0; i < 50000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>(0L);
            DeferredScalarSubscription<Integer> df = new DeferredScalarSubscription<>(tc);
            tc.onSubscribe(df);

            Runnable r1 = () -> df.request(1);

            TestHelper.race(r1, r1);

            tc.assertEmpty();
        }
    }
}
