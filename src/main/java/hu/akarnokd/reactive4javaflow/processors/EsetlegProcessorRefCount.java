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

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

final class EsetlegProcessorRefCount<T> extends EsetlegProcessor<T> implements RefCountAddRemoveSupport<T> {

    final EsetlegProcessor<T> actual;

    RefCountSubscriber<T>[] subscribers;
    static final VarHandle SUBSCRIBERS = VH.find(MethodHandles.lookup(), EsetlegProcessorRefCount.class, "subscribers", RefCountSubscriber[].class);

    static final RefCountSubscriber[] EMPTY = new RefCountSubscriber[0];
    static final RefCountSubscriber[] TERMINATED = new RefCountSubscriber[0];

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), EsetlegProcessorRefCount.class, "upstream", Flow.Subscription.class);

    public EsetlegProcessorRefCount(EsetlegProcessor<T> actual) {
        this.actual = actual;
        SUBSCRIBERS.setRelease(this, EMPTY);
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        RefCountSubscriber<T> parent = new RefCountSubscriber<>(s, this);
        if (add(parent)) {
            actual.subscribe(parent);
        } else {
            Throwable ex = actual.getThrowable();
            if (ex == null) {
                EmptySubscription.complete(s);
            } else {
                EmptySubscription.error(s, ex);
            }
        }
    }

    @Override
    public boolean hasThrowable() {
        return actual.hasThrowable();
    }

    @Override
    public Throwable getThrowable() {
        return actual.getThrowable();
    }

    @Override
    public boolean hasComplete() {
        return actual.hasComplete();
    }

    @Override
    public boolean hasSubscribers() {
        return actual.hasSubscribers();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (UPSTREAM.compareAndSet(this, null, subscription)) {
            actual.onSubscribe(subscription);
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void onNext(T item) {
        actual.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
        actual.onError(throwable);
    }

    @Override
    public void onComplete() {
        actual.onComplete();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean add(RefCountSubscriber<T> ps) {
        for (;;) {
            RefCountSubscriber<T>[] a = (RefCountSubscriber<T>[]) SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            RefCountSubscriber<T>[] b = new RefCountSubscriber[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ps;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void remove(RefCountSubscriber<T> ps) {
        for (;;) {
            RefCountSubscriber<T>[] a = (RefCountSubscriber<T>[]) SUBSCRIBERS.getAcquire(this);
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

            RefCountSubscriber<T>[] b;

            if (n == 1) {
                b = TERMINATED;
            } else {
                b = new RefCountSubscriber[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                if (b == TERMINATED) {
                    SubscriptionHelper.cancel(this, UPSTREAM);
                }
                break;
            }
        }
    }
}
