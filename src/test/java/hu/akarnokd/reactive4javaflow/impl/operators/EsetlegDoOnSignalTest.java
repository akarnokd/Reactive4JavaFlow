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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.hot.SolocastProcessor;
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class EsetlegDoOnSignalTest {
    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .doOnNext(v -> { }),
                 1);
    }

    @Test
    public void standardHide() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .hide()
                        .doOnNext(v -> { }),
                1);
    }

    @Test
    public void onError() {
        List<Throwable> errors = new ArrayList<>();
        Esetleg.error(new IOException())
                .doOnError(errors::add)
                .test()
                .assertFailure(IOException.class);

        TestHelper.assertError(errors, 0, IOException.class);
    }


    @Test
    public void onErrorConditional() {
        List<Throwable> errors = new ArrayList<>();
        Esetleg.error(new IOException())
                .doOnError(errors::add)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);

        TestHelper.assertError(errors, 0, IOException.class);
    }

    @Test
    public void onNext() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnNext(list::add)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(1), list);
    }


    @Test
    public void onNextConditional() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnNext(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(1), list);
    }

    @Test
    public void onAfterNext() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doAfterNext(list::add)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(1), list);
    }

    @Test
    public void onAfterNextConditional() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doAfterNext(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(1), list);
    }

    @Test
    public void onComplete() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnComplete(() -> list.add(100))
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onCompleteConditional() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnComplete(() -> list.add(100))
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(100), list);
    }


    @Test
    public void onRequest() {
        List<Long> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnRequest(list::add)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(Long.MAX_VALUE), list);
    }

    @Test
    public void onRequestConditional() {
        List<Long> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnRequest(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(Long.MAX_VALUE), list);
    }

    @Test
    public void onCancel() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnCancel(() -> list.add(100))
                .test(1, true, 0)
                .assertEmpty();

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onCancelConditional() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnCancel(() -> list.add(100))
                .filter(v -> true)
                .test(1, true, 0)
                .assertEmpty();

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onSubscribe() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnSubscribe(s -> list.add(100))
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onSubscribeConditional() {
        List<Integer> list = new ArrayList<>();
        Esetleg.just(1)
                .doOnSubscribe(s -> list.add(100))
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void macroFusedNormal() {
        List<Long> list = new ArrayList<>();
        Esetleg.just(1L)
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .test()
                .assertResult(1L);

        assertEquals(Arrays.asList(100L, 100L,
                Long.MAX_VALUE, Long.MAX_VALUE,
                1L, 1L, 1L, 1L,
                300L, 300L
        ), list);
    }


    @Test
    public void macroFusedTake() {
        List<Long> list = new ArrayList<>();
        Esetleg.just(1L)
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .test()
                .assertResult(1L);

        assertEquals(Arrays.asList(100L, 100L,
                Long.MAX_VALUE, Long.MAX_VALUE,
                1L, 1L, 1L, 1L,
                300L, 300L
        ), list);
    }

    @Test
    public void macroFusedError() {
        List<Long> list = new ArrayList<>();
        Esetleg.<Long>error(new IOException())
                .doOnNext(s -> { })
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .doOnSubscribe(s -> list.add(100L))
                .doOnNext(list::add)
                .doAfterNext(list::add)
                .doOnError(e -> list.add(200L))
                .doOnComplete(() -> list.add(300L))
                .doOnRequest(list::add)
                .doOnCancel(() -> list.add(400L))
                .test()
                .assertFailure(IOException.class);

        assertEquals(Arrays.asList(100L, 100L,
                Long.MAX_VALUE, Long.MAX_VALUE,
                200L, 200L
        ), list);
    }

    @Test
    public void fusedPollCrashSync() {
        List<Object> list = new ArrayList<>();
        new Esetleg<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }
        .doOnError(list::add)
                .test(0, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);

        assertTrue(list.toString(), list.get(0) instanceof IOException);
    }

    @Test
    public void fusedPollCrashASync() {
        List<Object> list = new ArrayList<>();
        new Esetleg<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }
                .doOnError(list::add)
                .test(0, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);

        assertTrue(list.toString(), list.get(0) instanceof IOException);
    }

    @Test
    public void fusedPollAndCallbackCrashSync() {
        new Esetleg<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }
                .doOnError(e -> { throw new IllegalArgumentException(); })
                .test(0, false, FusedSubscription.ANY)
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void onSubscribeCrash() {
            Esetleg.empty()
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .test()
                    .assertFailure(IOException.class);
    }

    @Test
    public void onSubscribeCrash2() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.error(new IllegalArgumentException())
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void onSubscribeCrashConditional() {
        Esetleg.empty()
                .doOnSubscribe(s -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onSubscribeCrashConditional2() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.error(new IllegalArgumentException())
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .filter(v -> true)
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void onErrorCrash() {
        Esetleg.error(new IOException())
                .doOnError(e -> { throw new IllegalArgumentException(); })
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void onErrorCrashConditional() {
        Esetleg.error(new IOException())
                .doOnError(e -> { throw new IllegalArgumentException(); })
                .filter(v -> true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IllegalArgumentException.class);
                });
    }

    @Test
    public void onCompleteCrash() {
        Esetleg.empty()
                .doOnComplete(() -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onCompleteCrashConditional() {
        Esetleg.empty()
                .doOnComplete(() -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onNextCrash() {
        Esetleg.just(1)
                .doOnNext(e -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onNextCrashConditional() {
        Esetleg.just(1)
                .doOnNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onNextCrashConditional2() {
        Esetleg.just(1).hide()
                .doOnNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onAfterNextCrash() {
        Esetleg.just(1)
                .doAfterNext(e -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void onAfterNextCrashConditional() {
        Esetleg.just(1)
                .doAfterNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void onAfterNextCrashConditional2() {
        Esetleg.just(1)
                .hide()
                .doAfterNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

}
