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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class FirstProcessor<T> extends EsetlegProcessor<T> {

    FirstProcessorSubscription<T>[] subscribers = EMPTY;
    static final VarHandle SUBSCRIBERS;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    boolean once;
    static final VarHandle ONCE;

    T value;
    Throwable error;

    static final FirstProcessorSubscription[] EMPTY = new FirstProcessorSubscription[0];
    static final FirstProcessorSubscription[] TERMINATED = new FirstProcessorSubscription[0];

    static {
        try {
            SUBSCRIBERS = MethodHandles.lookup().findVarHandle(FirstProcessor.class, "subscribers", FirstProcessorSubscription[].class);
            UPSTREAM = MethodHandles.lookup().findVarHandle(FirstProcessor.class, "upstream", Flow.Subscription.class);
            ONCE = MethodHandles.lookup().findVarHandle(FirstProcessor.class, "once", boolean.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    @Override
    public boolean hasThrowable() {
        return SUBSCRIBERS.getAcquire(this) == TERMINATED && error != null;
    }

    @Override
    public Throwable getThrowable() {
        return SUBSCRIBERS.getAcquire(this) == TERMINATED ? error : null;
    }

    @Override
    public boolean hasComplete() {
        return SUBSCRIBERS.getAcquire(this) == TERMINATED && error == null;
    }

    @Override
    public boolean hasSubscribers() {
        return ((FirstProcessorSubscription[])SUBSCRIBERS.getAcquire(this)).length != 0;
    }

    public boolean hasValue() {
        return SUBSCRIBERS.getAcquire(this) == TERMINATED && value != null;
    }

    public T getValue() {
        return SUBSCRIBERS.getAcquire(this) == TERMINATED ? value : null;
    }

    @SuppressWarnings("unchecked")
    boolean add(FirstProcessorSubscription<T> ps) {
        for (;;) {
            FirstProcessorSubscription<T>[] a = (FirstProcessorSubscription<T>[]) SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            FirstProcessorSubscription<T>[] b = new FirstProcessorSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ps;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(FirstProcessorSubscription<T> ps) {
        for (;;) {
            FirstProcessorSubscription<T>[] a = (FirstProcessorSubscription<T>[]) SUBSCRIBERS.getAcquire(this);
            int n = a.length;
            if (n == 0) {
                break;
            }

            int j = -1;
            for (int i = 0; i < n; i++) {
                if (ps == a[i]) {
                    j = i;
                    break;
                }
            }

            if (j < 0) {
                break;
            }

            FirstProcessorSubscription<T>[] b;

            if (n == 1) {
                b = EMPTY;
            } else {
                b = new FirstProcessorSubscription[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                break;
            }
        }
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        FirstProcessorSubscription<T> ps = new FirstProcessorSubscription<>(s, this);
        s.onSubscribe(ps);

        if (add(ps)) {
            if (ps.isCancelled()) {
                remove(ps);
            }
        } else {
            Throwable ex = error;
            if (ex != null) {
                ps.error(ex);
            } else {
                T v = value;
                if (v != null) {
                    ps.complete(v);
                } else {
                    ps.complete();
                }
            }
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (UPSTREAM.compareAndSet(this, null, subscription)) {
            subscription.request(Long.MAX_VALUE);
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void onNext(T item) {
        Objects.requireNonNull(item, "item == null");
        if (ONCE.compareAndSet(this, false, true)) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            this.value = item;
            for (FirstProcessorSubscription<T> s : (FirstProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                s.complete(item);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onError(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable == null");
        if (ONCE.compareAndSet(this, false, true)) {
            UPSTREAM.setRelease(this, SubscriptionHelper.CANCELLED);
            this.error = throwable;
            this.value = null;
            for (FirstProcessorSubscription<T> s : (FirstProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                s.error(throwable);
            }
            return;
        }
        FolyamPlugins.onError(throwable);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onComplete() {
        if (ONCE.compareAndSet(this, false, true)) {
            UPSTREAM.setRelease(this, SubscriptionHelper.CANCELLED);
            for (FirstProcessorSubscription<T> s : (FirstProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                s.complete();
            }
        }
    }

    static final class FirstProcessorSubscription<T> extends DeferredScalarSubscription<T> {

        final FirstProcessor<T> parent;

        public FirstProcessorSubscription(FolyamSubscriber<? super T> actual, FirstProcessor<T> parent) {
            super(actual);
            this.parent = parent;
        }

        @Override
        public void cancel() {
            super.cancel();
            parent.remove(this);
        }
    }
}
