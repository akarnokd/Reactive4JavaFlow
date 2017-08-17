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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import org.junit.Test;

import java.io.IOException;
import java.lang.invoke.*;
import java.util.concurrent.Flow;

public class HalfSerializerTest {

    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(HalfSerializer.class);
    }

    static final class HalfConsumer implements ConditionalSubscriber<Integer> {

        final TestConsumer<Integer> tc;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), HalfConsumer.class, "wip", int.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), HalfConsumer.class, "error", Throwable.class);

        HalfConsumer() {
            tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
        }

        @Override
        public boolean tryOnNext(Integer item) {
            tc.onNext(item);
            return item == 1;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Integer item) {
            tc.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            tc.onError(throwable);
        }

        @Override
        public void onComplete() {
            tc.onComplete();
        }
    }

    @Test
    public void onNextOnErrorRace() {
        for (int i = 0; i < 1000; i++) {
            TestHelper.withErrorTracking(errors -> {
                HalfConsumer hc = new HalfConsumer();

                Throwable ex = new IOException();

                Runnable r1 = () -> HalfSerializer.onNext(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, 1);
                Runnable r2 = () -> HalfSerializer.onError(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, ex);

                TestHelper.race(r1, r2);

                if (hc.tc.values().size() >= 1) {
                    hc.tc.assertFailure(IOException.class, 1);
                } else {
                    hc.tc.assertFailure(IOException.class);
                }

                if (!errors.isEmpty()) {
                    TestHelper.assertError(errors, 0, IOException.class);
                }
            });
        }
    }


    @Test
    public void tryOnNextOnErrorRace() {
        for (int i = 0; i < 1000; i++) {
            TestHelper.withErrorTracking(errors -> {
                HalfConsumer hc = new HalfConsumer();

                Throwable ex = new IOException();

                Runnable r1 = () -> HalfSerializer.tryOnNext(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, 1);
                Runnable r2 = () -> HalfSerializer.onError(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, ex);

                TestHelper.race(r1, r2);

                if (hc.tc.values().size() >= 1) {
                    hc.tc.assertFailure(IOException.class, 1);
                } else {
                    hc.tc.assertFailure(IOException.class);
                }

                if (!errors.isEmpty()) {
                    TestHelper.assertError(errors, 0, IOException.class);
                }
            });
        }
    }


    @Test
    public void onNextOnCompleteRace() {
        for (int i = 0; i < 1000; i++) {
            HalfConsumer hc = new HalfConsumer();

            Runnable r1 = () -> HalfSerializer.onNext(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, 1);
            Runnable r2 = () -> HalfSerializer.onComplete(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR);

            TestHelper.race(r1, r2);

            if (hc.tc.values().size() >= 1) {
                hc.tc.assertResult(1);
            } else {
                hc.tc.assertResult();
            }
        }
    }

    @Test
    public void tryOnNextOnCompleteRace() {
        for (int i = 0; i < 1000; i++) {
            HalfConsumer hc = new HalfConsumer();

            Runnable r1 = () -> HalfSerializer.tryOnNext(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, 1);
            Runnable r2 = () -> HalfSerializer.onComplete(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR);

            TestHelper.race(r1, r2);

            if (hc.tc.values().size() >= 1) {
                hc.tc.assertResult(1);
            } else {
                hc.tc.assertResult();
            }
        }
    }

    @Test
    public void onErrorOnCompleteRace() {
        for (int i = 0; i < 1000; i++) {
            TestHelper.withErrorTracking(errors -> {
                HalfConsumer hc = new HalfConsumer();

                Throwable ex = new IOException();

                Runnable r1 = () -> HalfSerializer.onComplete(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR);
                Runnable r2 = () -> HalfSerializer.onError(hc, hc, HalfConsumer.WIP, HalfConsumer.ERROR, ex);

                TestHelper.race(r1, r2);

                if (hc.tc.errors().size() >= 1) {
                    hc.tc.assertFailure(IOException.class);
                } else {
                    hc.tc.assertResult();
                }

                if (!errors.isEmpty()) {
                    TestHelper.assertError(errors, 0, IOException.class);
                }
            });
        }
    }

}
