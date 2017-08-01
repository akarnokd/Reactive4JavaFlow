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

package hu.akarnokd.reactive4javaflow.hot;

import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class LastProcessor<T> extends FolyamProcessor<T> {

    LastProcessorSubscription<T>[] subscribers = EMPTY;
    static final VarHandle SUBSCRIBERS;

    T value;
    Throwable error;

    static final LastProcessorSubscription[] EMPTY = new LastProcessorSubscription[0];
    static final LastProcessorSubscription[] TERMINATED = new LastProcessorSubscription[0];

    static {
        try {
            SUBSCRIBERS = MethodHandles.lookup().findVarHandle(LastProcessor.class, "subscribers", LastProcessorSubscription[].class);
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
        return ((LastProcessorSubscription[])SUBSCRIBERS.getAcquire(this)).length != 0;
    }

    @SuppressWarnings("unchecked")
    boolean add(LastProcessorSubscription<T> ps) {
        for (;;) {
            LastProcessorSubscription<T>[] a = (LastProcessorSubscription<T>[]) SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            LastProcessorSubscription<T>[] b = new LastProcessorSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ps;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(LastProcessorSubscription<T> ps) {
        for (;;) {
            LastProcessorSubscription<T>[] a = (LastProcessorSubscription<T>[]) SUBSCRIBERS.getAcquire(this);
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

            LastProcessorSubscription<T>[] b;

            if (n == 1) {
                b = EMPTY;
            } else {
                b = new LastProcessorSubscription[n - 1];
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
        LastProcessorSubscription<T> ps = new LastProcessorSubscription<>(s, this);
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
        if (SUBSCRIBERS.getAcquire(this) != TERMINATED) {
            subscription.request(Long.MAX_VALUE);
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void onNext(T item) {
        Objects.requireNonNull(item, "item == null");
        if (SUBSCRIBERS.getAcquire(this) != TERMINATED) {
            this.value = item;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onError(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable == null");
        if (SUBSCRIBERS.getAcquire(this) != TERMINATED) {
            if (error == null) {
                this.error = throwable;
                this.value = null;
                for (LastProcessorSubscription<T> s : (LastProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                    s.error(throwable);
                }
                return;
            }
        }
        FolyamPlugins.onError(throwable);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onComplete() {
        T v = value;
        if (v == null) {
            for (LastProcessorSubscription<T> s : (LastProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                s.complete();
            }
        } else {
            for (LastProcessorSubscription<T> s : (LastProcessorSubscription<T>[]) SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                s.complete(v);
            }
        }
    }

    static final class LastProcessorSubscription<T> extends DeferredScalarSubscription<T> {

        final LastProcessor<T> parent;

        public LastProcessorSubscription(FolyamSubscriber<? super T> actual, LastProcessor<T> parent) {
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
