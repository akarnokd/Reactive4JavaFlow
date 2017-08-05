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

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;

import java.util.concurrent.atomic.AtomicInteger;

public class DeferredScalarSubscription<T> extends AtomicInteger implements FusedSubscription<T> {

    protected final FolyamSubscriber<? super T> actual;

    protected T value;

    protected static final int NO_REQUEST_NO_VALUE = 0;
    protected static final int NO_REQUEST_HAS_VALUE = 1;
    protected static final int HAS_REQUEST_NO_VALUE = 2;
    protected static final int HAS_REQUEST_HAS_VALUE = 3;
    protected static final int CANCELLED = 4;
    protected static final int FUSED_NONE = 5;
    protected static final int FUSED_READY = 6;
    protected static final int FUSED_CONSUMED = 7;


    public DeferredScalarSubscription(FolyamSubscriber<? super T> actual) {
        this.actual = actual;
    }

    @Override
    public final int requestFusion(int mode) {
        if ((mode & ASYNC) != 0) {
            setRelease(FUSED_NONE);
            return ASYNC;
        }
        return NONE;
    }

    @Override
    public final T poll() throws Throwable {
        if (getAcquire() == FUSED_READY) {
            setRelease(FUSED_CONSUMED);
            return value;
        }
        return null;
    }

    @Override
    public final boolean isEmpty() {
        return getAcquire() != FUSED_READY;
    }

    @Override
    public final void clear() {
        if (compareAndSet(FUSED_READY, FUSED_CONSUMED)) {
            value = null;
        }
    }

    @Override
    public final void request(long n) {
        for (;;) {
            int s = getAcquire();
            if (s == NO_REQUEST_HAS_VALUE) {
                if (compareAndSet(NO_REQUEST_HAS_VALUE, HAS_REQUEST_HAS_VALUE)) {
                    T v = value;
                    actual.onNext(v);
                    if (getAcquire() != CANCELLED) {
                        actual.onComplete();
                    }
                }
            } else
            if (s == NO_REQUEST_NO_VALUE) {
                if (!compareAndSet(NO_REQUEST_NO_VALUE, HAS_REQUEST_NO_VALUE)) {
                    continue;
                }
            }
            break;
        }
    }

    @Override
    public void cancel() {
        setRelease(CANCELLED);
    }

    public final boolean isCancelled() {
        return getAcquire() == CANCELLED;
    }

    public final void complete(T value) {
        for (;;) {
            int s = getAcquire();
            if (s == HAS_REQUEST_NO_VALUE) {
                setRelease(HAS_REQUEST_HAS_VALUE);
                actual.onNext(value);
                if (getAcquire() != CANCELLED) {
                    actual.onComplete();
                }
            } else
            if (s == NO_REQUEST_NO_VALUE) {
                this.value = value;
                if (!compareAndSet(NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
                    continue;
                }
            } else
            if (s == FUSED_NONE) {
                this.value = value;
                setRelease(FUSED_READY);
                actual.onNext(null);
                if (getAcquire() != CANCELLED) {
                    actual.onComplete();
                }
            }
            break;
        }
    }

    public final void complete() {
        if (getAcquire() != CANCELLED) {
            setRelease(CANCELLED);
            actual.onComplete();
        }
    }

    public final void error(Throwable ex) {
        if (getAcquire() != CANCELLED) {
            setRelease(CANCELLED);
            actual.onError(ex);
        }
    }
}
