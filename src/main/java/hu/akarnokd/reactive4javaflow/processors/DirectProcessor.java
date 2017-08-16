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
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public final class DirectProcessor<T> extends FolyamProcessor<T> {

    DirectSubscription<T>[] subscribers = EMPTY;
    static final VarHandle SUBSCRIBERS;

    static final DirectSubscription[] EMPTY = new DirectSubscription[0];
    static final DirectSubscription[] TERMINATED = new DirectSubscription[0];

    Throwable error;

    static {
        try {
            SUBSCRIBERS = MethodHandles.lookup().findVarHandle(DirectProcessor.class, "subscribers", DirectSubscription[].class);
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
    @SuppressWarnings("unchecked")
    public boolean hasSubscribers() {
        return ((DirectSubscription<T>[])SUBSCRIBERS.getAcquire(this)).length != 0;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        DirectSubscription<T> parent = new DirectSubscription<>(s, this);
        s.onSubscribe(parent);
        if (add(parent)) {
            if (parent.getAcquire() == Long.MIN_VALUE) {
                remove(parent);
            }
        } else {
            if (parent.getAcquire() != Long.MIN_VALUE) {
                Throwable ex = error;
                if (ex == null) {
                    s.onComplete();
                } else {
                    s.onError(ex);
                }
            }
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SUBSCRIBERS.getAcquire(this) == TERMINATED) {
            subscription.cancel();
        } else {
            subscription.request(Long.MAX_VALUE);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(T item) {
        Objects.requireNonNull(item, "item == null");
        for (DirectSubscription<T> ds : (DirectSubscription<T>[])SUBSCRIBERS.getAcquire(this)) {
            ds.onNext(item);
        }
    }

    @SuppressWarnings("unchecked")
    public boolean tryOnNext(T item) {
        Objects.requireNonNull(item, "item == null");
        DirectSubscription<T>[] a = (DirectSubscription<T>[])SUBSCRIBERS.getAcquire(this);
        for (DirectSubscription<T> ds : a) {
            if (ds.getAcquire() == ds.emitted) {
                return false;
            }
        }
        for (DirectSubscription<T> ds : a) {
            ds.onNext(item);
        }
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onError(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable == null");
        Throwable ex = error;
        if (ex == null) {
            error = throwable;
            for (DirectSubscription<T> ds : (DirectSubscription<T>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                ds.onError(throwable);
            }
        } else {
            FolyamPlugins.onError(throwable);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onComplete() {
        for (DirectSubscription<T> ds : (DirectSubscription<T>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
            ds.onComplete();
        }
    }

    @SuppressWarnings("unchecked")
    boolean add(DirectSubscription<T> ds) {
        for (;;) {
            DirectSubscription<T>[] a = (DirectSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            DirectSubscription<T>[] b = new DirectSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ds;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(DirectSubscription<T> ds) {
        for (;;) {
            DirectSubscription<T>[] a = (DirectSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            int n = a.length;
            if (n == 0) {
                return;
            }
            int j = -1;
            for (int i = 0; i < n; i++) {
                if (ds == a[i]) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                break;
            }
            DirectSubscription<T>[] b;
            if (n == 1) {
                b = EMPTY;
            } else {
                b = new DirectSubscription[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                break;
            }
        }
    }

    static final class DirectSubscription<T> extends AtomicLong implements Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final DirectProcessor<T> parent;

        long emitted;

        DirectSubscription(FolyamSubscriber<? super T> actual, DirectProcessor<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequestedCancellable(this, n);
        }

        @Override
        public void cancel() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                parent.remove(this);
            }
        }

        void onNext(T item) {
            long r = getAcquire();
            if (r != Long.MIN_VALUE) {
                long e = emitted;
                if (r != e) {
                    emitted = e + 1;
                    actual.onNext(item);
                } else {
                    if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                        parent.remove(this);
                        actual.onError(new IllegalStateException("Flow.Subscriber is not ready to receive items."));
                    }
                }
            }
        }

        void onError(Throwable ex) {
            if (getAcquire() != Long.MIN_VALUE) {
                actual.onError(ex);
            }
        }

        void onComplete() {
            if (getAcquire() != Long.MIN_VALUE) {
                actual.onComplete();
            }
        }
    }
}
