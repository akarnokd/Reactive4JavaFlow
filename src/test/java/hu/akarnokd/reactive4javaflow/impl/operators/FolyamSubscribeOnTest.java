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
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FolyamSubscribeOnTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single()),
                1, 2, 3, 4, 5);
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .hide()
                        .subscribeOn(SchedulerServices.single()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .subscribeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .subscribeOn(SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }

    @Test
    public void subscribeRequestRace() {
        for (int i = 0; i < 1000; i++) {

            TestConsumer<Integer> tc = new TestConsumer<>(0L);

            FolyamSubscriber[] sub = { null };

            new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    sub[0] = s;
                }
            }.subscribeOn(SchedulerServices.trampoline())
            .subscribe(tc);

            BooleanSubscription bs = new BooleanSubscription();

            Runnable r1 = () -> {
                FolyamSubscriber<Integer> s = sub[0];
                s.onSubscribe(bs);
                s.onNext(1);
                s.onComplete();
            };

            Runnable r2 = () -> tc.requestMore(1);

            TestHelper.race(r1, r2);

            tc.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1);
        }
    }
}
