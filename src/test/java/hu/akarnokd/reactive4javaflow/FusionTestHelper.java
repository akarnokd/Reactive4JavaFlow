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

import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;

import java.util.NoSuchElementException;
import java.util.concurrent.*;

public final class FusionTestHelper {

    private FusionTestHelper() {
        throw new IllegalStateException("No instances!");
    }

    @SafeVarargs
    public static <T> void assertResult(Flow.Publisher<T> source, T... values) {
        TestConsumer<T> ts;

        // test normal consumption
        // -----------------------
        ts = new TestConsumer<>();

        source.subscribe(ts);

        ts.awaitDone(5, TimeUnit.SECONDS)
        .assertResult(values);

        if (values.length != 0) {
            // test initial no request
            // -----------------------
            ts = new TestConsumer<>(0);

            source.subscribe(ts);

                ts.assertEmpty()
                  .requestMore(Long.MAX_VALUE)
                  .awaitDone(5, TimeUnit.SECONDS)
                  .assertResult(values);

            // test initial no request, request exact
            // -----------------------
            ts = new TestConsumer<>(0);

            source.subscribe(ts);

            ts.assertEmpty()
                    .requestMore(values.length)
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values);

            // in-sequence cancel after first item
            // -----------------------------------
            ts = new TestConsumer<>() {
                @Override
                public void onNext(T item) {
                    super.onNext(item);
                    close();
                    onComplete();
                }
            };

            source.subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS)
              .assertResult(values[0]);

            // external cancel after first item
            // --------------------------------
            ts = new TestConsumer<>(1);

            source.subscribe(ts);

            ts.awaitCount(1, 10, 5000)
              .cancel()
              .assertValues(values[0])
              .assertNoErrors();

            if (values.length == 1) {
                ts.awaitDone(5, TimeUnit.SECONDS)
                  .assertComplete();
            }

            // check for longer sequences
            if (values.length > 1) {
                ts = new TestConsumer<>(0);

                source.subscribe(ts);

                for (int i = 0; i < values.length; i++) {
                    ts.requestMore(1)
                            .awaitCount(i + 1, 10, 5000);
                }

                ts.awaitDone(5, TimeUnit.SECONDS)
                  .assertResult(values);
            }
        }

        // test fused dynamic source
        // -------------------------

        if (source instanceof FusedDynamicSource) {
            FusedDynamicSource<T> f = (FusedDynamicSource<T>) source;

            T v;
            try {
                v = f.value();
            } catch (Throwable ex) {
                throw new AssertionError(ex);
            }

            if (v == null && values.length != 0) {
                throw new AssertionError("Source is empty.");
            }

            if (v != null) {
                if (values.length == 0) {
                    throw new AssertionError("Source has value: " + valueAndClass(v));
                }

                if (values.length != 1) {
                    throw new AssertionError("Source has a single value: " + valueAndClass(v) + " but " + values.length + " items expected");
                }

                if (!v.equals(values[0])) {
                    throw new AssertionError("Values differ. Expected: " + valueAndClass(values[0]) + ", Actual: " + valueAndClass(v));
                }
            }
        }

        // test fusion mode
        // ----------------
        ts = new TestConsumer<>();
        ts.requestFusionMode(FusedSubscription.ANY);

        source.subscribe(ts);

        ts.awaitDone(5, TimeUnit.SECONDS)
                .assertResult(values);

        if (values.length != 0) {
            TestConsumer<T> ts1 = new TestConsumer<>();

            source.subscribe(new FolyamSubscriber<T>() {
                FusedQueue<T> qs;
                Flow.Subscription upstream;
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    upstream = subscription;
                    ts1.onSubscribe(new BooleanSubscription());
                    if (subscription instanceof FusedSubscription) {
                        FusedSubscription<T> fs = (FusedSubscription<T>) subscription;

                        int m = fs.requestFusion(FusedSubscription.ANY);
                        if (m == FusedSubscription.SYNC) {
                            if (fs.isEmpty()) {
                                ts1.onError(new NoSuchElementException());
                            }
                            try {
                                T v = fs.poll();
                                if (v != null) {
                                    ts1.onNext(v);
                                }
                                ts1.onComplete();
                            } catch (Throwable ex) {
                                ts1.onError(ex);
                            }
                            fs.cancel();
                            fs.clear();
                            if (!fs.isEmpty()) {
                                ts1.onError(new IndexOutOfBoundsException("Elements not cleared"));
                            }
                            return;
                        }
                        if (m == FusedSubscription.ASYNC) {
                            qs = fs;
                        }
                    }
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(T item) {
                    FusedQueue<T> fs = qs;
                    if (fs != null) {
                        if (fs.isEmpty()) {
                            ts1.onError(new NoSuchElementException());
                        }
                        try {
                            T v = fs.poll();
                            if (v != null) {
                                ts1.onNext(v);
                            }
                            ts1.onComplete();
                        } catch (Throwable ex) {
                            ts1.onError(ex);
                        }
                        upstream.cancel();
                        fs.clear();
                        if (!fs.isEmpty()) {
                            ts1.onError(new IndexOutOfBoundsException("Elements not cleared"));
                        }
                    } else {
                        ts1.onNext(item);
                        upstream.cancel();
                        ts1.onComplete();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts1.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts1.onComplete();
                }
            });

            ts1.awaitDone(5, TimeUnit.SECONDS)
            .assertResult(values[0]);
        }
    }

    static String valueAndClass(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString() + " (" + o.getClass().getSimpleName() + ")";
    }

}
