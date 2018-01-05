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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class FolyamFlatMapTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.empty().flatMap(Folyam::just));
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.just(1).flatMap(Folyam::just), 1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.range(1, 5).flatMap(Folyam::just), 1, 2, 3, 4, 5);
    }

    @Test
    public void standard2Hidden() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .flatMap(v -> Folyam.just(v).hide()), 1, 2, 3, 4, 5);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .flatMap(v -> Folyam.range(v, 2)),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard3Hidden() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .flatMap(v -> Folyam.range(v, 2).hide()),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .flatMap(v -> Folyam.range(v, 2), 1),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard4Hidden() {
        TestHelper.assertResult(Folyam.range(1, 5)
                        .flatMap(v -> Folyam.range(v, 2).hide(), 1),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .flatMap(v -> Folyam.range(v, 2), 2),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard5Hidden() {
        TestHelper.assertResult(Folyam.range(1, 5)
                        .flatMap(v -> Folyam.range(v, 2).hide(), 2),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standard6() {
        TestHelper.assertResult(
                Folyam.range(1, 5).flatMap(Folyam::just, 1),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard7() {
        TestHelper.assertResult(
                Folyam.range(1, 5).flatMapDelayError(Folyam::just, 1),
                1, 2, 3, 4, 5);
    }

    @Test
    public void longScalar() {
        Folyam.range(1, 1000)
                .flatMap(Folyam::just)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longScalarHidden() {
        Folyam.range(1, 1000)
                .flatMap(v -> Folyam.just(v).hide())
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixed() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v) : Folyam.empty())
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixed2() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v).hide() : Folyam.empty())
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixed3() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v) : Folyam.empty().hide())
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longScalarMixed4() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v).hide() : Folyam.empty().hide())
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longScalarConditional() {
        Folyam.range(1, 1000)
                .flatMap(Folyam::just)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longScalarHiddenConditional() {
        Folyam.range(1, 1000)
                .flatMap(v -> Folyam.just(v).hide())
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixedConditional() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v) : Folyam.empty())
                .filter(v -> true)
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixed2Conditional() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v).hide() : Folyam.empty())
                .filter(v -> true)
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longScalarMixed3Conditional() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v) : Folyam.empty().hide())
                .filter(v -> true)
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longScalarMixed4Conditional() {
        Folyam.range(1, 1000)
                .flatMap(v -> v % 2 == 0 ? Folyam.just(v).hide() : Folyam.empty().hide())
                .filter(v -> true)
                .test()
                .assertValueCount(500)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1, f -> f.flatMap(Folyam::just), IOException.class);
    }

    @Test
    public void error2() {
        TestHelper.assertFailureComposed(-1, f -> f.flatMapDelayError(Folyam::just), IOException.class);
    }

    @Test
    public void error3() {
        TestHelper.assertFailureComposed(5, f -> f.flatMap(v -> {
            if (v >= 4) {
                return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        }), IOException.class, 1, 2, 3);
    }

    @Test
    public void error4() {
        TestHelper.assertFailureComposed(5, f -> f.flatMapDelayError(v -> {
            if (v == 4) {
                return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        }), IOException.class, 1, 2, 3, 5);
    }

    @Test
    public void exactRequestShouldComplete() {
        Folyam.range(1, 5)
                .flatMap(v -> Folyam.range(v, 2))
                .test(0)
                .assertEmpty()
                .requestMore(9)
                .requestMore(1)
                .assertResult(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void exactRequestShouldCompleteConditional() {
        Folyam.range(1, 5)
                .flatMap(v -> Folyam.range(v, 2))
                .filter(v -> true)
                .test(0)
                .assertEmpty()
                .requestMore(9)
                .requestMore(1)
                .assertResult(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void longAsync() {
        for (int i = 0; i < 1000; i++) {
            assertBoth(Folyam.range(1, 1000)
                    .flatMap(v -> Folyam.just(1).subscribeOn(SchedulerServices.computation())));
        }
    }

    @Test
    public void longAsync2() {
        for (int i = 0; i < 1000; i++) {
            assertBoth(Folyam.range(1, 1000)
                    .flatMap(v -> Folyam.just(1).observeOn(SchedulerServices.computation())));
        }
    }

    @Test
    public void mapperCrash() {
        TestHelper.assertFailureComposed(5, f -> f.flatMap(w -> { throw new IOException(); }), IOException.class);
    }

    @Test
    public void mixedScalarAndAsync() {
        assertBoth(Folyam.range(1, 1000)
                .flatMap(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.observeOn(SchedulerServices.computation());
                    }
                    return f;
                }));
    }

    @Test
    public void mixedScalarAndAsync2() {
        assertBoth(Folyam.range(1, 1000)
                .flatMap(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.observeOn(SchedulerServices.computation());
                    }
                    return f;
                }, 1));
    }

    @Test
    public void mixedScalarAndAsync3() {
        assertBoth(Folyam.range(1, 1000)
                .flatMap(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.subscribeOn(SchedulerServices.computation());
                    }
                    return f;
                }));
    }

    @Test
    public void mixedScalarAndAsync4() {
        assertBoth(Folyam.range(1, 1000)
                .flatMap(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.subscribeOn(SchedulerServices.computation());
                    }
                    return f;
                }, 1));
    }

    @Test
    public void mixedScalarAndAsync5() {
        assertBoth(Folyam.range(1, 1000)
                .flatMapDelayError(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.observeOn(SchedulerServices.computation());
                    }
                    return f;
                }));
    }

    @Test
    public void mixedScalarAndAsync6() {
        assertBoth(Folyam.range(1, 1000)
                .flatMapDelayError(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.subscribeOn(SchedulerServices.computation());
                    }
                    return f;
                }));
    }

    @Test
    public void mixedScalarAndAsync7() {
        assertBoth(Folyam.range(1, 2000)
                .flatMapDelayError(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.observeOn(SchedulerServices.computation());
                    }
                    return f;
                }).take(1000));
    }


    @Test
    public void mixedScalarAndAsync8() {
        assertBoth(Folyam.range(1, 2000)
                .flatMapDelayError(v -> {
                    Folyam<Integer> f = Folyam.just(1);
                    if (v % 2 == 0) {
                        f = f.subscribeOn(SchedulerServices.computation());
                    }
                    return f;
                }).take(1000));
    }

    void assertBoth(Folyam<?> f) {
        f.test()
        .withTag("Normal")
        .awaitDone(5, TimeUnit.SECONDS)
        .assertValueCount(1000)
        .assertNoErrors()
        .assertComplete();

        f
        .filter(v -> true)
        .test()
        .withTag("Conditional")
        .awaitDone(5, TimeUnit.SECONDS)
        .assertValueCount(1000)
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void innerError() {
        Folyam.just(1).hide()
                .flatMap(v -> Folyam.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorDelayed() {
        Folyam.just(1).hide()
                .flatMapDelayError(v -> Folyam.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void scalar1Queue() {
        Folyam.just(1).flatMap(v -> Folyam.just(1), 1)
                .test(0)
                .assertEmpty()
                .requestMore(1)
                .assertResult(1);
    }

    @Test
    public void pollCrash() {
        Folyam.just(1).hide()
                .flatMap(v -> new Folyam<Integer>() {
                    @Override
                    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                        s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                    }
                })
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void pollCrash2() {
        Folyam.just(1).hide()
                .flatMapDelayError(v -> new Folyam<Integer>() {
                    @Override
                    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                        s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                    }
                })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void pollCrashConditional() {
        Folyam.just(1).hide()
                .flatMap(v -> new Folyam<Integer>() {
                    @Override
                    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                        s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                    }
                })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void pollCrash2Conditional() {
        Folyam.just(1).hide()
                .flatMapDelayError(v -> new Folyam<Integer>() {
                    @Override
                    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                        s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                    }
                })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void crossMap() {
        for (int i = 1; i <= 1_000_000; i *= 10) {
            int j = 1_000_000 / i;

            Folyam.range(1, i)
                    .flatMap(v -> Folyam.range(v, j))
                    .test()
                    .withTag("Normal, i = " + i + ", j = " + j)
                    .assertValueCount(1_000_000)
                    .assertNoErrors()
                    .assertComplete();

            Folyam.range(1, i)
                    .flatMap(v -> Folyam.range(v, j))
                    .filter(v -> true)
                    .test()
                    .withTag("Conditional, i = " + i + ", j = " + j)
                    .assertValueCount(1_000_000)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void publisher() {
        Folyam.merge(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)))
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher2() {
        Folyam.merge(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)), 1)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher3() {
        Folyam.mergeDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)))
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher4() {
        Folyam.mergeDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)), 1)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }
    @Test
    public void publisher5() {
        Folyam.mergeDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.error(new IOException()), Folyam.range(4, 3)))
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher6() {
        Folyam.mergeDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.error(new IOException()), Folyam.range(4, 3)), 1)
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5, 6);
    }



    @Test
    public void failingFusedInnerCancelsSource() {
        final AtomicInteger counter = new AtomicInteger();
        Folyam.range(1, 5)
                .doOnNext(v -> counter.getAndIncrement())
                .flatMap((CheckedFunction<Integer, Folyam<Integer>>) v ->
                        Folyam.fromIterable(() -> new Iterator<Integer>() {
                            @Override
                            public boolean hasNext() {
                                return true;
                            }

                            @Override
                            public Integer next() {
                                throw new IllegalArgumentException();
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        }))
                .test()
                .assertFailure(IllegalArgumentException.class);

        assertEquals(1, counter.get());
    }
}
