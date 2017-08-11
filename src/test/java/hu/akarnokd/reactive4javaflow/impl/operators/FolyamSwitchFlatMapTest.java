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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;

public class FolyamSwitchFlatMapTest {

    @Test
    public void normal() {
        DirectProcessor<Integer> ps = new DirectProcessor<>();

        @SuppressWarnings("unchecked")
        final DirectProcessor<Integer>[] pss = new DirectProcessor[3];
        for (int i = 0; i < pss.length; i++) {
            pss[i] = new DirectProcessor<>();
        }

        TestConsumer<Integer> ts = ps
                .switchFlatMap(v -> pss[v], 2)
                .test();

        ps.onNext(0);
        ps.onNext(1);

        pss[0].onNext(1);
        pss[0].onNext(2);
        pss[0].onNext(3);

        pss[1].onNext(10);
        pss[1].onNext(11);
        pss[1].onNext(12);

        ps.onNext(2);

        assertFalse(pss[0].hasSubscribers());

        pss[0].onNext(4);

        pss[2].onNext(20);
        pss[2].onNext(21);
        pss[2].onNext(22);

        pss[1].onComplete();
        pss[2].onComplete();
        ps.onComplete();

        ts.assertResult(1, 2, 3, 10, 11, 12, 20, 21, 22);
    }

    @Test
    public void normalSimple() {
        Folyam.range(1, 5)
                .switchFlatMap(v -> Folyam.just(1), 1)
                .test()
                .assertResult(1, 1, 1, 1, 1);
    }

    @Test
    public void empty() {
        Folyam.empty()
                .switchFlatMap(v -> Folyam.just(1), 1)
                .test()
                .assertResult();
    }

    @Test
    public void emptyInner() {
        Folyam.range(1, 5)
                .switchFlatMap(v -> Folyam.empty(), 1)
                .test()
                .assertResult();
    }

    @Test
    public void emptyBackpressured() {
        Folyam.empty()
                .switchFlatMap(v -> Folyam.just(1), 1)
                .test(0)
                .assertResult();
    }

    @Test
    public void emptyInnerBackpressured() {
        Folyam.range(1, 5)
                .switchFlatMap(v -> Folyam.empty(), 1)
                .test(0)
                .assertResult();
    }

    @Test
    public void errorOuter() {
        Folyam.error(new IOException())
                .switchFlatMap(v -> Folyam.just(1), 1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorInner() {
        Folyam.just(1)
                .switchFlatMap(v -> Folyam.error(new IOException()), 1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void backpressure() {
        Folyam.range(1, 5)
                .switchFlatMap(v -> Folyam.just(1), 1)
                .rebatchRequests(1)
                .test()
                .assertResult(1, 1, 1, 1, 1);
    }

    @Test
    public void take() {
        Folyam.range(1, 5)
                .switchFlatMap(v -> Folyam.just(1), 1)
                .take(3)
                .test()
                .assertResult(1, 1, 1);
    }

    @Test
    public void mixed() {
        for (int i = 1; i < 33; i++) {
            Folyam.interval(2, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                    .switchFlatMap(v -> Folyam.interval(1, TimeUnit.MILLISECONDS, SchedulerServices.computation()), i)
                    .take(100)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(100)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void mixed2() {
        for (int i = 1; i < 33; i++) {
            Folyam.interval(2, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                    .switchFlatMap(v -> Folyam.interval(1, TimeUnit.MILLISECONDS, SchedulerServices.computation()).take(16), i)
                    .rebatchRequests(1)
                    .take(100)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(100)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void mixed3() {
        for (int i = 1; i < 33; i++) {
            Folyam.interval(2, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                    .switchFlatMap(v -> Folyam.interval(1, TimeUnit.MILLISECONDS, SchedulerServices.computation()).take(16), i, 16)
                    .rebatchRequests(1)
                    .take(100)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertValueCount(100)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void innerErrorRace() {
        final Throwable ex = new IOException();

        for (int i = 0; i < 1000; i++) {
            final DirectProcessor<Integer> pp1 = new DirectProcessor<>();
            final DirectProcessor<Integer> pp2 = new DirectProcessor<>();

            TestConsumer<Integer> ts = pp1
                    .switchFlatMap(v -> pp2, 2)
                    .test();

            pp1.onNext(1);

            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    pp1.onComplete();
                }
            };
            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    pp2.onError(ex);
                }
            };

            TestHelper.race(r1, r2);

            ts.assertFailure(IOException.class);
        }
    }

    @Test
    public void cancel() {
        final DirectProcessor<Integer> pp1 = new DirectProcessor<>();
        final DirectProcessor<Integer> pp2 = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp1
                .switchFlatMap(v -> pp2, 2)
                .test();

        pp1.onNext(1);

        ts.cancel();

        assertFalse(pp1.hasSubscribers());
        assertFalse(pp2.hasSubscribers());
    }

    @Test
    public void outerDoubleError() {
        TestHelper.withErrorTracking(errors -> {
            final DirectProcessor<Integer> pp2 = new DirectProcessor<>();

            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onError(new IOException());
                    s.onError(new IllegalArgumentException());
                }
            }
                    .switchFlatMap(v -> pp2, 2)
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void innerDoubleError() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.just(1)
                    .switchFlatMap(v -> new Folyam<Integer>() {
                        @Override
                        protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                            s.onSubscribe(new BooleanSubscription());
                            s.onError(new IOException());
                            s.onError(new IllegalArgumentException());
                        }
                    }, 2)
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void mapperThrows() {
        DirectProcessor<Integer> bp = new DirectProcessor<>();

        TestConsumer<Object> tc = bp
                .switchFlatMap(new CheckedFunction<Object, Flow.Publisher<Object>>() {
                    @Override
                    public Flow.Publisher<Object> apply(Object v) throws Exception {
                        throw new IOException();
                    }
                }, 2)
                .test();

        bp.onNext(1);

        tc.assertFailure(IOException.class);

        assertFalse(bp.hasSubscribers());
    }
}
