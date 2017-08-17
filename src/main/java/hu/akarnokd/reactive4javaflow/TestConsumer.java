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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.FusedQueue;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.VolatileSizeArrayList;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public class TestConsumer<T> implements FolyamSubscriber<T>, AutoDisposable {

    final List<T> items;

    final List<Throwable> errors;

    final CountDownLatch cdl;

    volatile int completions;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    long requested;
    static final VarHandle REQUESTED;

    FusedQueue<T> qs;

    int requestedFusionMode;

    int actualFusionMode;

    String tag;

    boolean timeout;

    static {
        try {
            UPSTREAM = MethodHandles.lookup().findVarHandle(TestConsumer.class, "upstream", Flow.Subscription.class);
            REQUESTED = MethodHandles.lookup().findVarHandle(TestConsumer.class, "requested", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public TestConsumer() {
        this(Long.MAX_VALUE);
    }

    public TestConsumer(long initialRequest) {
        this.items = new VolatileSizeArrayList<>();
        this.errors = new VolatileSizeArrayList<>();
        this.cdl = new CountDownLatch(1);
        this.requested = initialRequest;
    }

    @Override
    public final void onSubscribe(Flow.Subscription subscription) {
        if (subscription == null) {
            errors.add(new NullPointerException("subscription == null in TestConsumer"));
            return;
        }
        if (UPSTREAM.compareAndSet(this, null, subscription)) {
            if (subscription instanceof FusedSubscription) {
                @SuppressWarnings("unchecked")
                FusedSubscription<T> qs = (FusedSubscription<T>)subscription;
                int f = requestedFusionMode;
                if (f != 0) {
                    int m = qs.requestFusion(f);
                    actualFusionMode = m;
                    if (m == FusedSubscription.SYNC) {
                        T v;

                        for (;;) {
                            try {
                                v = qs.poll();
                            } catch (Throwable ex) {
                                close();
                                errors.add(ex);
                                cdl.countDown();
                                return;
                            }

                            if (v == null) {
                                completions++;
                                cdl.countDown();
                                return;
                            }
                            items.add(v);
                        }
                    } else
                    if (m == FusedSubscription.ASYNC) {
                        this.qs = qs;
                    }
                }
            } else {
                actualFusionMode = -1;
            }
            long r = (long)REQUESTED.getAndSet(this, 0L);
            if (r != 0L) {
                subscription.request(r);
            }
        } else {
            subscription.cancel();
            if (!SubscriptionHelper.isCancelled(this, UPSTREAM)) {
                errors.add(new IllegalStateException("onSubscribe called again in TestConsumer"));
            }
        }
    }

    @Override
    public void onNext(T item) {
        if (upstream == null) {
            UPSTREAM.compareAndSet(this, null, MissingSubscription.MISSING);
            errors.add(new IllegalStateException("onSubscribe was not called before onNext in TestConsumer"));
        }
        if (actualFusionMode > 0) {
            if (actualFusionMode == FusedSubscription.SYNC) {
                close();
                errors.add(new IllegalStateException("Should not call onNext in SYNC mode."));
            } else {
                T v;
                for (; ; ) {
                    try {
                        v = qs.poll();
                    } catch (Throwable ex) {
                        close();
                        qs.clear();
                        errors.add(ex);
                        cdl.countDown();
                        return;
                    }
                    if (v == null) {
                        break;
                    }
                    items.add(v);
                }
            }
        } else {
            if (item == null) {
                errors.add(new NullPointerException("item == null in TestConsumer"));
            } else {
                items.add(item);
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (upstream == null) {
            UPSTREAM.compareAndSet(this, null, MissingSubscription.MISSING);
            errors.add(new IllegalStateException("onSubscribe was not called before onError in TestConsumer"));
        }
        if (throwable == null) {
            throwable = new NullPointerException("throwable == null in TestConsumer");
        }
        errors.add(throwable);
        cdl.countDown();
    }

    @Override
    public void onComplete() {
        if (upstream == null) {
            UPSTREAM.compareAndSet(this, null, MissingSubscription.MISSING);
            errors.add(new IllegalStateException("onSubscribe was not called before onComplete in TestConsumer"));
        }
        if (++completions > 1) {
            errors.add(new IllegalStateException("onComplete called again: " + completions));
        }
        cdl.countDown();
    }

    @Override
    public final void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
    }

    AssertionError fail(String message) {
        StringBuilder b = new StringBuilder();

        b.append(message);

        b.append(" (")
        .append("items: ").append(items.size())
        .append(", errors: ").append(errors.size())
        .append(", completions: ").append(completions)
        .append(", latch: ").append(cdl.getCount())
        ;
        if (timeout) {
            b.append(", timeout!");
        }
        if (SubscriptionHelper.isCancelled(this, UPSTREAM)) {
            b.append(", cancelled!");
        }
        if (tag != null) {
            b.append(", tag: ").append(tag);
        }
        b.append(")");

        AssertionError ex = new AssertionError(b.toString());
        int c = errors.size();
        for (int i = 0; i < c; i++) {
            ex.addSuppressed(errors.get(i));
        }
        return ex;
    }

    public final TestConsumer<T> requestFusionMode(int mode) {
        this.requestedFusionMode = mode;
        return this;
    }

    public final TestConsumer<T> assertFusionMode(int mode) {
        if (this.actualFusionMode != mode) {
            throw fail("Wrong fusion mode. Expected: " + fusionMode(mode) + ", Actual: " + fusionMode(actualFusionMode));
        }
        return this;
    }

    static String fusionMode(int mode) {
        if (mode == FusedSubscription.NONE) {
            return "NONE";
        }
        if (mode == FusedSubscription.SYNC) {
            return "SYNC";
        }
        if (mode == FusedSubscription.ASYNC) {
            return "ASYNC";
        }
        if (mode == -1) {
            return "Not supported";
        }
        return "??? " + mode;
    }

    public final TestConsumer<T> awaitDone(long timeout, TimeUnit unit) {
        try {
            if (!cdl.await(timeout, unit)) {
                this.timeout = true;
                close();
            }
        } catch (InterruptedException ex) {
            close();
            throw fail("Wait interrupted");
        }
        return this;
    }

    @SafeVarargs
    public final TestConsumer<T> assertValues(T... expected) {
        int c = items.size();
        if (c != expected.length) {
            throw fail("Number of items differ. Expected: " + expected.length + ", Actual: " + c);
        }
        for (int i = 0; i < c; i++) {
            Object exp = expected[i];
            Object act = items.get(i);
            if (!Objects.equals(exp, act)) {
                throw fail("Item #" + i + " differs. Expected: " + valueAndClass(exp) + ", Actual: " + valueAndClass(act));
            }
        }
        return this;
    }

    public final TestConsumer<T> assertNoErrors() {
        if (!errors.isEmpty()) {
            throw fail("Error(s) present.");
        }
        return this;
    }

    public final TestConsumer<T> assertNotComplete() {
        if (completions != 0) {
            throw fail("Completed.");
        }
        return this;
    }

    public final TestConsumer<T> assertComplete() {
        int c = completions;
        if (c == 0) {
            throw fail("Not completed.");
        }
        if (c > 1) {
            throw fail("Multiple completions.");
        }
        return this;
    }

    public final TestConsumer<T> assertOnSubscribe() {
        if (upstream == null) {
            throw fail("onSubscribe not called.");
        }
        return this;
    }

    public final TestConsumer<T> assertError(Class<? extends Throwable> errorClass) {
        int c = errors.size();
        if (c == 0) {
            throw fail("No errors.");
        }
        for (int i = 0; i < c; i++) {
            if (errorClass.isInstance(errors.get(i))) {
                if (c == 1) {
                    return this;
                }
                throw fail("Error present but not alone.");
            }
        }
        throw fail("Error not present.");
    }

    static String valueAndClass(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString() + " (" + o.getClass().getSimpleName() + ")";
    }

    public final TestConsumer<T> assertErrorMessage(String message) {
        int c = errors.size();
        if (c == 0) {
            throw fail("No errors.");
        }
        String msg = errors.get(0).getMessage();
        if (Objects.equals(message, msg)) {
            if (c != 1) {
                throw fail("Message present but other errors as well.");
            }
        } else {
            throw fail("Messages differ. Expected: " + message + ", Actual: " + msg);
        }
        return this;
    }

    @SafeVarargs
    public final TestConsumer<T> assertResult(T... expected) {
        assertOnSubscribe();
        assertValues(expected);
        assertNoErrors();
        assertComplete();
        return this;
    }

    @SafeVarargs
    public final TestConsumer<T> assertFailure(Class<? extends Throwable> errorClass, T... expected) {
        assertOnSubscribe();
        assertValues(expected);
        assertError(errorClass);
        assertNotComplete();
        return this;
    }


    @SafeVarargs
    public final TestConsumer<T> assertFailureAndMessage(Class<? extends Throwable> errorClass, String message, T... expected) {
        assertOnSubscribe();
        assertValues(expected);
        assertError(errorClass);
        assertErrorMessage(message);
        assertNotComplete();
        return this;
    }

    public final TestConsumer<T> assertEmpty() {
        assertOnSubscribe();
        assertValues();
        assertNoErrors();
        assertNotComplete();
        return this;
    }

    public final TestConsumer<T> awaitCount(int expected, long delayStep, long delayTotal) {
        long start = System.currentTimeMillis();
        while (items.size() < expected && cdl.getCount() != 0 && start + delayTotal > System.currentTimeMillis()) {
            try {
                Thread.sleep(delayStep);
            } catch (InterruptedException ex) {
                close();
                break;
            }
        }
        return this;
    }

    public final TestConsumer<T> withTag(String tag) {
        this.tag = tag;
        return this;
    }

    public final String getTag() {
        return tag;
    }

    public final TestConsumer<T> requestMore(long n) {
        if (actualFusionMode == FusedSubscription.SYNC) {
            throw fail("Requesting in SYNC fused mode is forbidden.");
        }
        SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        return this;
    }

    public final TestConsumer<T> cancel() {
        close();
        return this;
    }

    public final TestConsumer<T> assertInnerErrors(Consumer<List<Throwable>> consumer) {
        if (errors.size() == 0) {
            throw fail("No errors.");
        }
        List<Throwable> errorsList = new ArrayList<>();
        errors.forEach(e -> {
            if (e instanceof CompositeThrowable) {
                errorsList.addAll(Arrays.asList(e.getSuppressed()));
            } else {
                errorsList.add(e);
            }
        });
        consumer.accept(errorsList);
        return this;
    }

    public final TestConsumer<T> assertValueAt(int index, T item) {
        int s = items.size();
        if (s <= index) {
            throw fail("Not enough elements: " + index);
        }
        T v = items.get(index);
        if (!Objects.equals(item, v)) {
            throw fail("Item @ " + index + " differs. Expected: " + valueAndClass(item) + ", Actual: " + valueAndClass(v));
        }
        return this;
    }

    public final TestConsumer<T> assertNoTimeout() {
        if (timeout) {
            throw fail("Timeout.");
        }
        return this;
    }

    public final TestConsumer<T> assertValueCount(int expected) {
        int s = items.size();
        if (s != expected) {
            throw fail("Number of items differ. Expected: " + expected + ", Actual: " + s);
        }
        return this;
    }

    public final TestConsumer<T> clear() {
        items.clear();
        return this;
    }

    public final TestConsumer<T> assertValueSet(Collection<T> expected) {
        int s = items.size();
        if (s != expected.size()) {
            throw fail("Number of items differ. Expected: " + expected.size() + ", Actual: " + s);
        }
        for (int i = 0; i < items.size(); i++) {
            T v = items.get(i);
            if (!expected.contains(v)) {
                throw fail("Item @ " + i + " not expected: " + valueAndClass(v));
            }
        }
        return this;
    }

    public final List<T> values() {
        return items;
    }

    public final TestConsumer<T> forEach(BiConsumer<Integer, T> onItem) {
        int s = items.size();
        for (int i = 0; i < s; i++) {
            onItem.accept(i, items.get(i));
        }
        return this;
    }

    public final List<Throwable> errors() {
        return errors;
    }

    enum MissingSubscription implements Flow.Subscription {
        MISSING;

        @Override
        public void request(long n) {
            // deliberately no-op
        }

        @Override
        public void cancel() {
            // deliberately no-op
        }
    }
}
