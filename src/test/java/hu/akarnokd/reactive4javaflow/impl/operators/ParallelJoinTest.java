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

import java.io.IOException;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

public class ParallelJoinTest {

    @Test
    public void overflowFastpath() {
        new ParallelFolyam<Integer>() {
            @Override
            public void subscribeActual(FolyamSubscriber<? super Integer>[] subscribers) {
                subscribers[0].onSubscribe(new BooleanSubscription());
                subscribers[0].onNext(1);
                subscribers[0].onNext(2);
                subscribers[0].onNext(3);
            }

            @Override
            public int parallelism() {
                return 1;
            }
        }
        .sequential(1)
        .test(0)
        .assertFailure(IllegalStateException.class);
    }

    @Test
    public void overflowSlowpath() {
        @SuppressWarnings("unchecked")
        final FolyamSubscriber<? super Integer>[] subs = new FolyamSubscriber[1];

        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                subs[0].onNext(2);
                subs[0].onNext(3);
            }
        };

        new ParallelFolyam<Integer>() {
            @Override
            public void subscribeActual(FolyamSubscriber<? super Integer>[] subscribers) {
                subs[0] = subscribers[0];
                subscribers[0].onSubscribe(new BooleanSubscription());
                subscribers[0].onNext(1);
            }

            @Override
            public int parallelism() {
                return 1;
            }
        }
        .sequential(1)
        .subscribe(ts);

        ts.assertFailure(IllegalStateException.class, 1);
    }

    @Test
    public void emptyBackpressured() {
        Folyam.empty()
        .parallel()
        .sequential()
        .test(0)
        .assertResult();
    }

    @Test
    public void overflowFastpathDelayError() {
        new ParallelFolyam<Integer>() {
            @Override
            public void subscribeActual(FolyamSubscriber<? super Integer>[] subscribers) {
                subscribers[0].onSubscribe(new BooleanSubscription());
                subscribers[0].onNext(1);
                subscribers[0].onNext(2);
            }

            @Override
            public int parallelism() {
                return 1;
            }
        }
        .sequentialDelayError(1)
        .test(0)
        .requestMore(1)
        .assertFailure(IllegalStateException.class, 1);
    }

    @Test
    public void overflowSlowpathDelayError() {
        @SuppressWarnings("unchecked")
        final FolyamSubscriber<? super Integer>[] subs = new FolyamSubscriber[1];

        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                if (t == 1) {
                    subs[0].onNext(2);
                    subs[0].onNext(3);
                }
            }
        };

        new ParallelFolyam<Integer>() {
            @Override
            public void subscribeActual(FolyamSubscriber<? super Integer>[] subscribers) {
                subs[0] = subscribers[0];
                subscribers[0].onSubscribe(new BooleanSubscription());
                subscribers[0].onNext(1);
            }

            @Override
            public int parallelism() {
                return 1;
            }
        }
        .sequentialDelayError(1)
        .subscribe(ts);

        ts.requestMore(1);

        ts.assertFailure(IllegalStateException.class, 1, 2);
    }

    @Test
    public void emptyBackpressuredDelayError() {
        Folyam.empty()
        .parallel()
        .sequentialDelayError()
        .test(0)
        .assertResult();
    }

    @Test
    public void delayError() {
        TestConsumer<Integer> flow = Folyam.range(1, 2)
        .parallel(2)
        .map(new CheckedFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .sequentialDelayError()
        .test()
        .assertFailure(CompositeThrowable.class)
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, IOException.class);
            TestHelper.assertError(errors, 1, IOException.class);
        });
    }

    @Test
    public void normalDelayError() {
        Folyam.just(1)
        .parallel(1)
        .sequentialDelayError(1)
        .test()
        .assertResult(1);
    }

    @Test
    public void rangeDelayError() {
        Folyam.range(1, 2)
        .parallel(1)
        .sequentialDelayError(1)
        .take(1)
        .test()
        .assertResult(1);
    }

    @Test
    public void rangeDelayErrorBackpressure() {
        Folyam.range(1, 3)
        .parallel(1)
        .sequentialDelayError(1)
        .take(2)
        .rebatchRequests(1)
        .test()
        .assertResult(1, 2);
    }

    @Test
    public void rangeDelayErrorBackpressure2() {
        Folyam.range(1, 3)
        .parallel(1)
        .sequentialDelayError(1)
        .rebatchRequests(1)
        .test()
        .assertResult(1, 2, 3);
    }

    @Test
    public void delayErrorCancelBackpressured() {
        TestConsumer<Integer> ts = Folyam.range(1, 3)
        .parallel(1)
        .sequentialDelayError(1)
        .test(0);

        ts
        .cancel();

        ts.assertEmpty();
    }

    @Test
    public void delayErrorCancelBackpressured2() {
        TestConsumer<Integer> ts = Folyam.<Integer>empty()
        .parallel(1)
        .sequentialDelayError(1)
        .test(0);

        ts.assertResult();
    }

    @Test
    public void consumerCancelsAfterOne() {
        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                cancel();
                onComplete();
            }
        };

        Folyam.range(1, 3)
        .parallel(1)
        .sequential()
        .subscribe(ts);

        ts.assertResult(1);
    }

    @Test
    public void delayErrorConsumerCancelsAfterOne() {
        TestConsumer<Integer> ts = new TestConsumer<Integer>(1) {
            @Override
            public void onNext(Integer t) {
                super.onNext(t);
                cancel();
                onComplete();
            }
        };

        Folyam.range(1, 3)
        .parallel(1)
        .sequentialDelayError()
        .subscribe(ts);

        ts.assertResult(1);
    }

    @Test
    public void delayErrorDrainTrigger() {
        Folyam.range(1, 3)
        .parallel(1)
        .sequentialDelayError()
        .test(0)
        .requestMore(1)
        .assertValues(1)
        .requestMore(1)
        .assertValues(1, 2)
        .requestMore(1)
        .assertResult(1, 2, 3);
    }

    @Test
    public void failedRailIsIgnored() {
        Folyam.range(1, 4)
        .parallel(2)
        .map(new CheckedFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer v) throws Exception {
                if (v == 1) {
                    throw new IOException();
                }
                return v;
            }
        })
        .sequentialDelayError()
        .test()
        .assertFailure(IOException.class, 2, 3, 4);
    }

    @Test
    public void failedRailIsIgnoredHidden() {
        Folyam.range(1, 4).hide()
        .parallel(2)
        .map(new CheckedFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer v) throws Exception {
                if (v == 1) {
                    throw new IOException();
                }
                return v;
            }
        })
        .sequentialDelayError()
        .test()
        .assertFailure(IOException.class, 2, 3, 4);
    }
}
