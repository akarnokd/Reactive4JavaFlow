/*
 * Copyright 2016-2017 David Karnok
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

package hu.akarnokd.reactive4javaflow;

import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class EsetlegTest {

    @Test
    public void subscribeStrict() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        Esetleg.just(1)
                .subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        tc.onSubscribe(subscription);
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
                });
        tc.assertResult(1);
    }


    @Test
    public void safeSubscribeStrict() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        Esetleg.just(1)
                .safeSubscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        tc.onSubscribe(subscription);
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
                });
        tc.assertResult(1);
    }

    @Test
    public void subscribeActualCrash() {
        TestHelper.withErrorTracking(errors -> {
            new Esetleg<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    throw new IllegalArgumentException();
                }
            }
            .test();

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void compose() {
        Esetleg.just(1)
                .compose(v -> Esetleg.just(2))
                .test()
                .assertResult(2);
    }

    @Test
    public void subscribe0() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.just(1)
                    .subscribe();

            assertTrue(errors.isEmpty());
        });
    }

    @Test
    public void subscribe1() {
        TestHelper.withErrorTracking(errors -> {
            Object[] value = { null };
            Esetleg.just(1)
                    .subscribe(v -> { value[0] = v; });

            assertEquals(1, value[0]);
            assertTrue(errors.isEmpty());
        });
    }

    @Test
    public void subscribe2() {
        TestHelper.withErrorTracking(errors -> {
            Object[] value = { null, null };
            Esetleg.just(1)
                    .subscribe(v -> value[0] = v, e -> value[1] = e);

            assertEquals(1, value[0]);
            assertNull(value[1]);
            assertTrue(errors.isEmpty());
        });
    }

    @Test
    public void subscribeWith() {
        Esetleg.just(1)
                .subscribeWith(new TestConsumer<>())
                .assertResult(1);
    }

    @Test
    public void fromOptional() {
        Esetleg.fromOptional(Optional.of(1))
                .test()
                .assertResult(1);
    }

    @Test
    public void fromOptionalEmpty() {
        Esetleg.fromOptional(Optional.empty())
                .test()
                .assertResult();
    }

    @Test
    public void fromPublisher() {
        Esetleg<Integer> e = Esetleg.just(1);
        assertSame(e, Esetleg.fromPublisher(e));
    }

    @Test
    public void fromPublisher0() {
        Esetleg.fromPublisher(Folyam.empty())
                .test()
                .assertResult();
    }

    @Test
    public void fromPublisher1() {
        Esetleg.fromPublisher(Folyam.just(1))
                .test()
                .assertResult(1);
    }

    @Test
    public void fromPublisherError() {
        Esetleg.fromPublisher(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fromPublisherTooMany() {
        Esetleg.fromPublisher(Folyam.range(1, 5))
                .test(1)
                .assertFailure(IndexOutOfBoundsException.class);
    }

    @Test
    public void startWith() {
        Esetleg.just(1).startWith(Folyam.range(1, 5))
                .test()
                .assertResult(1, 2, 3, 4, 5, 1);
    }

    @Test
    public void mergeWith() {
        Esetleg<Integer> e = Esetleg.just(1).subscribeOn(SchedulerServices.computation());

        e.mergeWith(e)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 1);
    }

    @Test
    public void toFolyam() {
        Esetleg.just(1)
                .toFolyam()
                .parallel()
                .map(v -> v + 1)
                .sequential()
                .test()
                .assertResult(2);
    }

    @Test
    public void blockingGet() {
        assertEquals(1, Esetleg.just(1).blockingGet().get().intValue());
    }

    @Test
    public void blockingGetTimeout() {
        assertEquals(1, Esetleg.just(1).blockingGet(1, TimeUnit.MINUTES).get().intValue());
    }

    @Test
    public void blockingGetDefault() {
        assertEquals(2, Esetleg.empty().blockingGet(2));
    }

    @Test
    public void blockingSubscribe0() {
        Esetleg.just(1).blockingSubscribe();
    }

    @Test
    public void blockingSubscribe0Interrupt() {
        TestHelper.withErrorTracking(errors -> {
            Thread.currentThread().interrupt();
            try {
                Esetleg.never().blockingSubscribe();
            } finally {
                Thread.interrupted();
            }

            TestHelper.assertError(errors, 0, InterruptedException.class);
        });
    }

    @Test
    public void blockingSubscribe1() {
        Object[] vals = { null, null, null };

        Esetleg.just(1).blockingSubscribe(v -> vals[0] = v);

        assertEquals(1, vals[0]);
    }

    @Test
    public void blockingSubscribe2() {
        Object[] vals = { null, null, null };

        Esetleg.just(1).blockingSubscribe(v -> vals[0] = v, e -> vals[1] = e);

        assertEquals(1, vals[0]);
        assertNull(vals[1]);
    }


    @Test
    public void blockingSubscribe3() {
        Object[] vals = { null, null, null };

        Esetleg.just(1).blockingSubscribe(v -> vals[0] = v, e -> vals[1] = e, () -> vals[2] = 100);

        assertEquals(1, vals[0]);
        assertNull(vals[1]);
        assertEquals(100, vals[2]);
    }

    @Test
    public void blockingIterable() {
        for (Integer v : Esetleg.just(1).blockingIterable()) {
            assertEquals(1, v.intValue());
        }
    }

    @Test
    public void blockingStream() {
        List<Integer> list = Esetleg.just(1).blockingStream().collect(Collectors.toList());

        assertEquals(List.of(1), list);
    }

    @Test
    public void toCompletableFuture() throws Exception {
        assertEquals(1, Esetleg.just(1).subscribeOn(SchedulerServices.single())
                .toCompletableFuture()
                .get().intValue());
    }

    @Test
    public void publish() {
        ConnectableFolyam<Integer> cf = Esetleg.just(1).publish();

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test();

        cf.connect();

        tc1.assertResult(1);
        tc2.assertResult(1);
    }

    @Test
    public void replay() {
        ConnectableFolyam<Integer> cf = Esetleg.just(1).replay();

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test();

        cf.connect();

        tc1.assertResult(1);
        tc2.assertResult(1);

        cf.test().assertResult(1);
    }

    @Test
    public void cache() {
        int[] subs = { 0 };
        Esetleg<Integer> e = Esetleg.just(1)
                .doOnSubscribe(s -> ++subs[0])
                .cache();

        e.test().assertResult(1);
        e.test().assertResult(1);
        e.test().assertResult(1);

        assertEquals(1, subs[0]);
    }

    @Test
    public void publishSelector() {
        Esetleg.just(1)
                .publish(f -> f.mergeWith(f).sumInt(v -> v))
                .test()
                .assertResult(2);
    }

    @Test
    public void publishSelectorCrash() {
        Esetleg.just(1)
                .publish(f -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void publishSelectorConditional() {
        Esetleg.just(1)
                .publish(f -> f.mergeWith(f).sumInt(v -> v))
                .filter(v -> true)
                .test()
                .assertResult(2);
    }

    @Test
    public void replaySelector() {
        Esetleg.just(1)
                .replay(f -> f.concatWith(f).sumInt(v -> v))
                .test()
                .assertResult(2);
    }

    @Test
    public void replaySelectorConditional() {
        Esetleg.just(1)
                .replay(f -> f.concatWith(f).sumInt(v -> v))
                .filter(v -> true)
                .test()
                .assertResult(2);
    }

    @Test
    public void replaySelectorCrash() {
        Esetleg.just(1)
                .replay(f -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void multicastSelector() {
        Esetleg.just(1)
                .multicast(Esetleg::replay, f -> f.concatWith(f).sumInt(v -> v))
                .test()
                .assertResult(2);
    }

    @Test
    public void multicastSelectorConditional() {
        Esetleg.just(1)
                .multicast(Esetleg::replay, f -> f.concatWith(f).sumInt(v -> v))
                .filter(v -> true)
                .test()
                .assertResult(2);
    }

    @Test
    public void multicastSelectorCrash() {
        Esetleg.just(1)
                .multicast(Esetleg::replay, f -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }
}
