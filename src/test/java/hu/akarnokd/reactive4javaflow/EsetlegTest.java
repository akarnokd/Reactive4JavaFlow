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
import java.util.Optional;
import java.util.concurrent.Flow;

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
}
