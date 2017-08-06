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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FolyamDoOnSignalTest {
    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .doOnNext(v -> { }),
                 1, 2, 3, 4, 5);
    }

    @Test
    public void standardHide() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .hide()
                        .doOnNext(v -> { }),
                1, 2, 3, 4, 5);
    }

    @Test
    public void onError() {
        List<Throwable> errors = new ArrayList<>();
        Folyam.error(new IOException())
                .doOnError(errors::add)
                .test()
                .assertFailure(IOException.class);

        TestHelper.assertError(errors, 0, IOException.class);
    }


    @Test
    public void onErrorConditional() {
        List<Throwable> errors = new ArrayList<>();
        Folyam.error(new IOException())
                .doOnError(errors::add)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);

        TestHelper.assertError(errors, 0, IOException.class);
    }

    @Test
    public void onNext() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnNext(list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
    }


    @Test
    public void onNextConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnNext(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
    }

    @Test
    public void onAfterNext() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doAfterNext(list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
    }

    @Test
    public void onAfterNextConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doAfterNext(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
    }

    @Test
    public void onComplete() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnComplete(() -> list.add(100))
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onCompleteConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnComplete(() -> list.add(100))
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Collections.singletonList(100), list);
    }


    @Test
    public void onRequest() {
        List<Long> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnRequest(list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Collections.singletonList(Long.MAX_VALUE), list);
    }

    @Test
    public void onRequestConditional() {
        List<Long> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnRequest(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertEquals(Collections.singletonList(Long.MAX_VALUE), list);
    }

    @Test
    public void onCancel() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnCancel(() -> list.add(100))
                .take(3)
                .test()
                .assertResult(1, 2, 3);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onCancelConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnCancel(() -> list.add(100))
                .filter(v -> true)
                .take(3)
                .test()
                .assertResult(1, 2, 3);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onSubscribe() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnSubscribe(s -> list.add(100))
                .take(3)
                .test()
                .assertResult(1, 2, 3);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void onSubscribeConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5)
                .doOnSubscribe(s -> list.add(100))
                .filter(v -> true)
                .take(3)
                .test()
                .assertResult(1, 2, 3);

        assertEquals(Collections.singletonList(100), list);
    }

    @Test
    public void macroFusedNormal() {
        List<Long> list = new ArrayList<>();
        Folyam.rangeLong(1, 5)
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
                .assertResult(1L, 2L, 3L, 4L, 5L);

        assertEquals(Arrays.asList(100L, 100L,
                Long.MAX_VALUE, Long.MAX_VALUE,
                1L, 1L, 1L, 1L,
                2L, 2L, 2L, 2L,
                3L, 3L, 3L, 3L,
                4L, 4L, 4L, 4L,
                5L, 5L, 5L, 5L,
                300L, 300L
        ), list);
    }


    @Test
    public void macroFusedTake() {
        List<Long> list = new ArrayList<>();
        Folyam.rangeLong(1, 5)
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
                .take(3)
                .test()
                .assertResult(1L, 2L, 3L);

        assertEquals(Arrays.asList(100L, 100L,
                Long.MAX_VALUE, Long.MAX_VALUE,
                1L, 1L, 1L, 1L,
                2L, 2L, 2L, 2L,
                3L, 3L, 400L, 400L, 3L, 3L
        ), list);
    }

    @Test
    public void macroFusedError() {
        List<Long> list = new ArrayList<>();
        Folyam.<Long>error(new IOException())
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
        new Folyam<Integer>() {
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
        new Folyam<Integer>() {
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
        new Folyam<Integer>() {
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
    public void cancelRequestCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.range(1, 5)
                    .doOnRequest(r -> { throw new IllegalArgumentException(); })
                    .doOnCancel(() -> { throw new IOException(); })
                    .take(3)
                    .test()
                    .assertResult(1, 2, 3);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
            TestHelper.assertError(errors, 1, IOException.class);
        });
    }

    @Test
    public void onSubscribeCrash() {
            Folyam.empty()
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .test()
                    .assertFailure(IOException.class);
    }

    @Test
    public void onSubscribeCrash2() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IllegalArgumentException())
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void onSubscribeCrashConditional() {
        Folyam.empty()
                .doOnSubscribe(s -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onSubscribeCrashConditional2() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IllegalArgumentException())
                    .doOnSubscribe(s -> { throw new IOException(); })
                    .filter(v -> true)
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void onErrorCrash() {
        Folyam.error(new IOException())
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
        Folyam.error(new IOException())
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
        Folyam.empty()
                .doOnComplete(() -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onCompleteCrashConditional() {
        Folyam.empty()
                .doOnComplete(() -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onNextCrash() {
        Folyam.range(1, 5)
                .doOnNext(e -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onNextCrashConditional() {
        Folyam.range(1, 5)
                .doOnNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onNextCrashConditional2() {
        Folyam.range(1, 5).hide()
                .doOnNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void onAfterNextCrash() {
        Folyam.range(1, 5)
                .doAfterNext(e -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void onAfterNextCrashConditional() {
        Folyam.range(1, 5)
                .doAfterNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void onAfterNextCrashConditional2() {
        Folyam.range(1, 5)
                .hide()
                .doAfterNext(e -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void asyncFusedOnNext() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        TestConsumer<Integer> tc = sp.doOnNext(v -> { })
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void asyncFusedOnNextConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        TestConsumer<Integer> tc = sp.doOnNext(v -> { })
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void asyncFusedOnNextConditional2() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        TestConsumer<Integer> tc = sp
                .rebatchRequests(128)
                .doOnNext(v -> { })
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        tc.assertResult(1, 2, 3, 4, 5);
    }
}
