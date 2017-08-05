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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class BlockingLambdaConsumerTest {
    @Test
    public void normalSync() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .blockingSubscribe(tc::onNext);

        tc.assertValues(1, 2, 3, 4, 5)
                .assertNoErrors()
                .assertNotComplete();
    }

    @Test
    public void normalSync2() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalSync3() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .hide()
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalASync1() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext);

        tc.assertValues(1, 2, 3, 4, 5)
                .assertNoErrors()
                .assertNotComplete();
    }

    @Test
    public void normalASync2() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext);

        tc.assertValues(1, 2, 3, 4, 5)
                .assertNoErrors()
                .assertNotComplete();
    }

    @Test
    public void normalASync3() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normalASync4() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .observeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalASync5() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single())
                .hide()
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normalASync6() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 5)
                .observeOn(SchedulerServices.single())
                .hide()
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void errorNoConsumer() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IOException())
                    .blockingSubscribe(v -> { });

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void errorWithConsumer() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());

            Folyam.<Integer>error(new IOException())
                    .blockingSubscribe(tc::onNext, tc::onError);

            tc.assertFailure(IOException.class);

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 5000)
    public void longSource() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.range(1, 1000)
                .subscribeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test(timeout = 1000)
    public void interrupt() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Thread.currentThread().interrupt();
        try {
            Folyam.<Integer>never()
                    .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

            tc.assertFailure(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void onErrorFails() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IOException("Main"))
                    .blockingSubscribe(v -> { }, e -> { throw new IOException("Handler"); });

            TestHelper.assertError(errors, 0, IOException.class, "Handler");
        });
    }

    @Test
    public void onNextFails() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        Folyam.just(1)
                .blockingSubscribe(v -> { throw new IOException(); }, tc::onError, tc::onComplete);

        tc.assertFailure(IOException.class);
    }

    @Test
    public void onCompleteFails() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.empty()
                    .blockingSubscribe(v -> { }, e -> {}, () -> { throw new IOException("Handler"); });

            TestHelper.assertError(errors, 0, IOException.class, "Handler");
        });
    }

    @Test
    public void emptySync() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.<Integer>empty()
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult();
    }


    @Test
    public void emptySync1() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.<Integer>empty().hide()
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult();
    }

    @Test
    public void emptyAsync1() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.<Integer>empty()
                .subscribeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult();
    }


    @Test
    public void emptyAsync2() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        Folyam.<Integer>empty()
                .observeOn(SchedulerServices.single())
                .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertResult();
    }

    @Test
    public void syncFusedPollFails() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }
        .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertFailure(IOException.class);
    }


    @Test
    public void asyncFusedPollFails() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.ASYNC));
            }
        }
        .blockingSubscribe(tc::onNext, tc::onError, tc::onComplete);

        tc.assertFailure(IOException.class);
    }
}
