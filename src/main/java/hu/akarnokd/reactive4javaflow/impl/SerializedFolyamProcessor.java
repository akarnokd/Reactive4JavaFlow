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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.processors.FolyamProcessor;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class SerializedFolyamProcessor<T> extends FolyamProcessor<T> implements Flow.Subscription {

    final FolyamProcessor<T> actual;

    Flow.Subscription upstream;

    boolean emitting;
    boolean missed;
    boolean done;

    List<T> queue;
    Throwable error;

    public SerializedFolyamProcessor(FolyamProcessor<T> actual) {
        this.actual = actual;
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
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        actual.subscribe(s);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        upstream = subscription;
        actual.onSubscribe(this);
    }

    @Override
    public void onNext(T item) {
        if (item == null) {
            onError(new NullPointerException("item == null"));
            return;
        }
        synchronized (this) {
            if (emitting) {
                if (!done) {
                    List<T> q = queue;
                    if (q == null) {
                        q = new ArrayList<>();
                        queue = q;
                    }
                    q.add(item);
                    missed = true;
                }
                return;
            }
            emitting = true;
        }

        actual.onNext(item);

        for (;;) {
            List<T> q;
            boolean d;
            Throwable ex;
            synchronized (this) {
                if (!missed) {
                    missed = false;
                    emitting = false;
                    return;
                }
                missed = false;
                q = queue;
                queue = null;
                d = done;
                ex = error;
            }

            for (T v : q) {
                actual.onNext(v);
            }

            if (d) {
                if (ex == null) {
                    actual.onComplete();
                } else {
                    actual.onError(ex);
                }
                return;
            }
        }

    }

    @Override
    public void onError(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable == null");
        synchronized (this) {
            if (emitting) {
                if (!done && error == null) {
                    error = throwable;
                    done = true;
                    missed = true;
                }
                return;
            }
            emitting = true;
            done = true;
        }

        actual.onError(throwable);
    }

    @Override
    public void onComplete() {
        synchronized (this) {
            if (emitting) {
                done = true;
                missed = true;
                return;
            }
            emitting = true;
            done = true;
        }

        actual.onComplete();
    }

    @Override
    public void request(long n) {
        upstream.request(n);
    }

    @Override
    public void cancel() {
        upstream.cancel();
    }
}
