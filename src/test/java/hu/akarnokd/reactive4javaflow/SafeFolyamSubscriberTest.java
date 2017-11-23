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

package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SafeFolyamSubscriberTest {

    @Test
    public void onSubscribeCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.just(1)
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            throw new IllegalArgumentException("Forced failure");
                        }

                        @Override
                        public void onNext(Integer item) {
                            errors.add(new IllegalArgumentException("onNext"));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            errors.add(new IllegalArgumentException("onError"));
                        }

                        @Override
                        public void onComplete() {
                            errors.add(new IllegalArgumentException("onComplete"));
                        }
                    });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "Forced failure");
            assertEquals(errors.toString(), 1, errors.size());
        });
    }

    @Test
    public void onNextCrash() {
        TestHelper.withErrorTracking(errors -> {
            List<Throwable> items = new ArrayList<>();
            Folyam.just(1)
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(Integer item) {
                            throw new IllegalArgumentException("Forced failure onNext");
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            items.add(throwable);
                        }

                        @Override
                        public void onComplete() {
                            items.add(new IOException());
                        }
                    });

            TestHelper.assertError(items, 0, IllegalArgumentException.class, "Forced failure onNext");
            assertEquals(1, items.size());

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test
    public void onErrorCrash() {
        TestHelper.withErrorTracking(errors -> {
            List<Throwable> items = new ArrayList<>();
            Folyam.<Integer>error(new IOException("Outer failure"))
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(Integer item) {
                            items.add(new IllegalArgumentException("Forced failure onNext"));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            throw new IllegalArgumentException("Forced failure onError");
                        }

                        @Override
                        public void onComplete() {
                            items.add(new IOException("onComplete error"));
                        }
                    });

            TestHelper.assertError(errors, 0, CompositeThrowable.class);
            assertEquals(1, errors.size());

            List<Throwable> inner = Arrays.asList(errors.get(0).getSuppressed());
            TestHelper.assertError(inner, 0, IOException.class, "Outer failure");
            TestHelper.assertError(inner, 1, IllegalArgumentException.class, "Forced failure onError");
            assertEquals(2, inner.size());

            assertTrue(items.toString(), items.isEmpty());
        });
    }

    @Test
    public void onCompleteCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>empty()
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(Integer item) {
                            errors.add(new IllegalArgumentException("onNext"));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            errors.add(new IllegalArgumentException("onError"));
                        }

                        @Override
                        public void onComplete() {
                            throw new IllegalArgumentException("onComplete");
                        }
                    });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "onComplete");
            assertEquals(errors.toString(), 1, errors.size());
        });
    }

    @Test
    public void requestCrash() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                            throw new IllegalArgumentException("request");
                        }

                        @Override
                        public void cancel() {

                        }
                    });
                }
            }
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            subscription.request(1);
                        }

                        @Override
                        public void onNext(Integer item) {
                            errors.add(new IllegalArgumentException("onNext"));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            errors.add(new IllegalArgumentException("onError"));
                        }

                        @Override
                        public void onComplete() {
                            errors.add(new IllegalArgumentException("onComplete"));
                        }
                    });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "request");
            assertEquals(errors.toString(), 1, errors.size());
        });
    }

    @Test
    public void cancelCrash() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                        }

                        @Override
                        public void cancel() {
                            throw new IllegalArgumentException("cancel");
                        }
                    });
                }
            }
                    .safeSubscribe(new FolyamSubscriber<>() {
                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            subscription.cancel();
                        }

                        @Override
                        public void onNext(Integer item) {
                            errors.add(new IllegalArgumentException("onNext"));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            errors.add(new IllegalArgumentException("onError"));
                        }

                        @Override
                        public void onComplete() {
                            errors.add(new IllegalArgumentException("onComplete"));
                        }
                    });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "cancel");
            assertEquals(errors.toString(), 1, errors.size());
        });
    }

    @Test
    public void protocol() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> ts = new TestConsumer<>();

            new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onComplete();
                    s.onNext(1);
                    s.onError(new IOException("failure"));
                    s.onComplete();
                }
            }
            .safeSubscribe(ts);

            ts.assertResult();

            TestHelper.assertError(errors, 0, IOException.class, "failure");
            assertEquals(errors.toString(), 1, errors.size());
        });
    }

    @Test
    public void normal() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.range(1, 5)
                .safeSubscribe(ts);

        ts.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.<Integer>error(new IOException())
                .safeSubscribe(ts);

        ts.assertFailure(IOException.class);
    }

    @Test
    public void normalFlowSubscriber() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.range(1, 5)
                .safeSubscribe(new Flow.Subscriber<>() {

                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        ts.onSubscribe(new BooleanSubscription());
                        subscription.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Integer item) {
                        ts.onNext(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        ts.onError(throwable);
                    }

                    @Override
                    public void onComplete() {
                        ts.onComplete();
                    }
                });

        ts.assertResult(1, 2, 3, 4, 5);
    }
}
