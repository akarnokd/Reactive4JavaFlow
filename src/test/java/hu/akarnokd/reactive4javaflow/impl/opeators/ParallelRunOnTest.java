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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import hu.akarnokd.reactive4javaflow.impl.schedulers.ImmediateSchedulerService;
import org.junit.Test;

public class ParallelRunOnTest {

    @Test
    public void subscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .runOn(SchedulerServices.computation()));
    }

    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .runOn(ImmediateSchedulerService.INSTANCE)
                    .sequential()
                    .test()
                    .assertFailure(IOException.class);

            assertFalse(errors.isEmpty());
            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void conditionalPath() {
        Folyam.range(1, 1000)
        .parallel(2)
        .runOn(SchedulerServices.computation())
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                return v % 2 == 0;
            }
        })
        .sequential()
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertValueCount(500)
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void missingBackpressure() {
        new ParallelFolyam<Integer>() {
            @Override
            public int parallelism() {
                return 1;
            }

            @Override
            public void subscribeActual(FolyamSubscriber<? super Integer>[] subscribers) {
                subscribers[0].onSubscribe(new BooleanSubscription());
                subscribers[0].onNext(1);
                subscribers[0].onNext(2);
                subscribers[0].onNext(3);
            }
        }
        .runOn(ImmediateSchedulerService.INSTANCE, 1)
        .sequential(1)
        .test(0)
        .assertFailure(IllegalStateException.class);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void errorBackpressured() {
        Folyam.error(new IOException())
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .sequential(1)
        .test(0)
        .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void errorConditionalBackpressured() {
        TestConsumer<Object> ts = new TestConsumer<Object>(0L);

        Folyam.error(new IOException())
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .filter(v -> true)
        .subscribe(new FolyamSubscriber[] { ts });

        ts
        .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void emptyConditionalBackpressured() {
        TestConsumer<Object> ts = new TestConsumer<Object>(0L);

        Folyam.empty()
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .filter(v -> true)
        .subscribe(new FolyamSubscriber[] { ts });

        ts
        .assertResult();
    }

    @Test
    public void nextCancelRace() {
        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = pp.parallel(1)
            .runOn(SchedulerServices.computation())
            .sequential()
            .test();

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onNext(1);
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void nextCancelRaceBackpressured() {
        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = new TestConsumer<>(0L);

            pp.parallel(1)
            .runOn(SchedulerServices.computation())
            .subscribe(new FolyamSubscriber[] { ts });

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onNext(1);
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void nextCancelRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = pp.parallel(1)
            .runOn(SchedulerServices.computation())
            .filter(v -> true)
            .sequential()
            .test();

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onNext(1);
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void nextCancelRaceBackpressuredConditional() {
        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = new TestConsumer<>(0L);

            pp.parallel(1)
            .runOn(SchedulerServices.computation())
            .filter(v -> true)
            .subscribe(new FolyamSubscriber[] { ts });

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp.onNext(1);
                }
            };

            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    ts.cancel();
                }
            };

            TestHelper.race(r1, r2);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void normalCancelAfterRequest1() {

        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                cancel();
                onComplete();
            }
        };

        Folyam.range(1, 5)
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .subscribe(new FolyamSubscriber[] { ts });

        ts.assertResult(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void conditionalCancelAfterRequest1() {

        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                cancel();
                onComplete();
            }
        };

        Folyam.range(1, 5)
        .parallel(1)
        .runOn(ImmediateSchedulerService.INSTANCE)
        .filter(v -> true)
        .subscribe(new FolyamSubscriber[] { ts });

        ts.assertResult(1);
    }
}
