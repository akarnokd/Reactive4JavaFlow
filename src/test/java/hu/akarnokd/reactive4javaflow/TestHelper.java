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

import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import hu.akarnokd.reactive4javaflow.fused.FusedQueue;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import hu.akarnokd.reactive4javaflow.impl.operators.FolyamHide;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public final class TestHelper {

    private TestHelper() {
        throw new IllegalStateException("No instances!");
    }

    @SafeVarargs
    public static <T> void assertResult(Flow.Publisher<T> source, T... values) {
        assertResultInternal(source, values);
        assertResultInternal(FolyamPlugins.onAssembly(new FolyamHide<>(source)), values);
    }

    @SafeVarargs
    static <T> void assertResultInternal(Flow.Publisher<T> source, T... values) {
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

            // in-sequence cancel after first item, backpressure
            // -----------------------------------
            if (values.length > 2) {
                ts = new TestConsumer<>(2) {
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
            }
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
            @SuppressWarnings("unchecked")
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
            // regular fused
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
                            try {
                                if (fs.poll() != null) {
                                    ts1.onError(new IndexOutOfBoundsException("poll() returned new elements"));
                                }
                            } catch (Throwable ex) {
                                ts1.onError(ex);
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
                        try {
                            if (fs.poll() != null) {
                                ts1.onError(new IndexOutOfBoundsException("poll() returned new elements"));
                            }
                        } catch (Throwable ex) {
                            ts1.onError(ex);
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

            // regular fused
            TestConsumer<T> ts2 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {
                FusedQueue<T> qs;
                Flow.Subscription upstream;
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    upstream = subscription;
                    ts2.onSubscribe(new BooleanSubscription());
                    if (subscription instanceof FusedSubscription) {
                        @SuppressWarnings("unchecked")
                        FusedSubscription<T> fs = (FusedSubscription<T>) subscription;

                        int m = fs.requestFusion(FusedSubscription.ANY);
                        if (m == FusedSubscription.SYNC) {
                            if (fs.isEmpty()) {
                                ts2.onError(new NoSuchElementException());
                            }
                            try {
                                T v = fs.poll();
                                if (v != null) {
                                    ts2.onNext(v);
                                }
                                ts2.onComplete();
                            } catch (Throwable ex) {
                                ts2.onError(ex);
                            }
                            fs.cancel();
                            fs.clear();
                            if (!fs.isEmpty()) {
                                ts2.onError(new IndexOutOfBoundsException("Elements not cleared"));
                            }
                            try {
                                if (fs.poll() != null) {
                                    ts2.onError(new IndexOutOfBoundsException("poll() returned new elements"));
                                }
                            } catch (Throwable ex) {
                                ts2.onError(ex);
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
                public boolean tryOnNext(T item) {
                    FusedQueue<T> fs = qs;
                    if (fs != null) {
                        if (fs.isEmpty()) {
                            ts2.onError(new NoSuchElementException());
                        }
                        try {
                            T v = fs.poll();
                            if (v != null) {
                                ts2.onNext(v);
                            }
                            ts2.onComplete();
                        } catch (Throwable ex) {
                            ts2.onError(ex);
                        }
                        upstream.cancel();
                        fs.clear();
                        if (!fs.isEmpty()) {
                            ts2.onError(new IndexOutOfBoundsException("Elements not cleared"));
                        }
                        try {
                            if (fs.poll() != null) {
                                ts2.onError(new IndexOutOfBoundsException("poll() returned new elements"));
                            }
                        } catch (Throwable ex) {
                            ts2.onError(ex);
                        }
                    } else {
                        ts2.onNext(item);
                        upstream.cancel();
                        ts2.onComplete();
                    }
                    return true;
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts2.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts2.onComplete();
                }
            });

            ts2.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values[0]);
        }

        // test conditional source
        if (values.length != 0) {
            // unbounded request
            TestConsumer<T> ts1 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                @Override
                public boolean tryOnNext(T item) {
                    ts1.onNext(item);
                    return true;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts1.onSubscribe(new BooleanSubscription());
                    upstream = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
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
                    .assertResult(values);

            // one by one
            TestConsumer<T> ts2 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                @Override
                public boolean tryOnNext(T item) {
                    ts2.onNext(item);
                    return false;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts2.onSubscribe(new BooleanSubscription());
                    upstream = subscription;
                    subscription.request(1);
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts2.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts2.onComplete();
                }
            });

            ts2.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values);

            // every other one
            TestConsumer<T> ts3 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                int index;

                @Override
                public boolean tryOnNext(T item) {
                    ts3.onNext(item);
                    return ((index++) & 1) == 0;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts3.onSubscribe(new BooleanSubscription());
                    upstream = subscription;
                    subscription.request(values.length / 2 + 1);
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts3.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts3.onComplete();
                }
            });

            ts3.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values);

            // cancel after first, unbounded
            TestConsumer<T> ts4 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                int index;

                @Override
                public boolean tryOnNext(T item) {
                    ts4.onNext(item);
                    upstream.cancel();
                    ts4.onComplete();
                    return ((index++) & 1) == 0;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts4.onSubscribe(new BooleanSubscription());
                    upstream = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts4.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts4.onComplete();
                }
            });

            ts4.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values[0]);

            // cancel after first, bounded

            TestConsumer<T> ts5 = new TestConsumer<>();

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                int index;

                @Override
                public boolean tryOnNext(T item) {
                    ts5.onNext(item);
                    upstream.cancel();
                    ts5.onComplete();
                    return ((index++) & 1) == 0;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts5.onSubscribe(new BooleanSubscription());
                    upstream = subscription;
                    subscription.request(2);
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts5.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts5.onComplete();
                }
            });

            ts5.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values[0]);

            // one by one

            TestConsumer<T> ts6 = new TestConsumer<>(0);

            source.subscribe(new ConditionalSubscriber<T>() {

                Flow.Subscription upstream;

                int index;

                @Override
                public boolean tryOnNext(T item) {
                    ts6.onNext(item);
                    return true;
                }

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    upstream = subscription;
                    ts6.onSubscribe(new Flow.Subscription() {

                        @Override
                        public void request(long n) {
                            subscription.request(n);
                        }

                        @Override
                        public void cancel() {
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(T item) {
                    if (!tryOnNext(item)) {
                        upstream.request(1);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    ts6.onError(throwable);
                }

                @Override
                public void onComplete() {
                    ts6.onComplete();
                }
            });

            ts6.assertEmpty();

            for (int i = 0; i < values.length; i++) {
                ts6.requestMore(1)
                        .awaitCount(i + 1, 10, 5000);
            }

            ts6.awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(values);


        }
    }

    static String valueAndClass(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString() + " (" + o.getClass().getSimpleName() + ")";
    }

    public static List<Throwable> trackErrors() {
        List<Throwable> list = Collections.synchronizedList(new ArrayList<>());

        FolyamPlugins.setOnError(list::add);

        return list;
    }

    public static void checkUtilityClass(Class<?> clazz) {
        try {
            Constructor c = clazz.getDeclaredConstructor();
            c.setAccessible(true);
            c.newInstance();

        } catch (Throwable ex) {
            if ((ex.getCause() instanceof IllegalStateException)
                    && ex.getCause().getMessage().equals("No instances!")) {
                return;
            }
            throw new AssertionError(ex);
        }
        throw new AssertionError("Not an utility class!");
    }

    public static <E extends Enum<E>> void checkEnum(Class<E> e) {
        Enum<?>[] o = e.getEnumConstants();
        for (Enum<?> a : o) {
            assertNotNull(a.name());
            assertTrue(a.ordinal() >= 0);
        }
    }

    public static void assertError(List<Throwable> list, int index, Class<? extends Throwable> errorClazz) {
        Throwable ex = list.get(index);
        if (!errorClazz.isInstance(ex)) {
            throw new AssertionError("Wrong error: " + ex, ex);
        }
    }

    public static void assertError(List<Throwable> list, int index, Class<? extends Throwable> errorClazz, String message) {
        Throwable ex = list.get(index);
        if (!errorClazz.isInstance(ex)) {
            throw new AssertionError("Wrong error: " + ex, ex);
        }
        if (!Objects.equals(message, ex.getMessage())) {
            throw new AssertionError("Messages differ. Expected: " + message + ", actual: " + ex.getMessage());
        }
    }

    public static void withErrorTracking(CheckedConsumer<List<Throwable>> test) {
        List<Throwable> errors = trackErrors();
        try {
            test.accept(errors);
        } catch (Throwable ex) {
            throw new AssertionError(ex);
        } finally {
            FolyamPlugins.setOnError(null);
        }
    }

    @SafeVarargs
    public static <R> void folyamDonePath(Function<? super Folyam<Integer>, Flow.Publisher<R>> compose, R... result) {
        withErrorTracking(errors -> {
            Folyam<Integer> f = new Folyam<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onNext(1);
                    s.onComplete();
                    s.onError(new IOException("folyamDonePath"));
                    s.onComplete();
                    s.onNext(2);
                }
            };

            TestConsumer<R> tc = new TestConsumer<>();

            compose.apply(f).subscribe(tc);

            tc.assertResult(result);

            assertError(errors, 0, IOException.class, "folyamDonePath");
        });
    }

    @SafeVarargs
    public static <R> void esetlegDonePath(Function<? super Esetleg<Integer>, Flow.Publisher<R>> compose, R... result) {
        withErrorTracking(errors -> {
            Esetleg<Integer> f = new Esetleg<Integer>() {

                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onNext(1);
                    s.onComplete();
                    s.onError(new IOException("folyamDonePath"));
                    s.onComplete();
                    s.onNext(2);
                }
            };

            TestConsumer<R> tc = new TestConsumer<>();

            compose.apply(f).subscribe(tc);

            tc.assertResult(result);

            assertError(errors, 0, IOException.class, "folyamDonePath");
        });
    }
}
