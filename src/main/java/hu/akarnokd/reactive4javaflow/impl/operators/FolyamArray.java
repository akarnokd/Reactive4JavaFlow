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

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamArray<T> extends Folyam<T> {

    final T[] array;

    final int start;

    final int end;

    public FolyamArray(T[] array, int start, int end) {
        this.array = array;
        this.start = start;
        this.end = end;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new RangeConditionalSubscription((ConditionalSubscriber<? super T>)s, array, start, end));
        } else {
            s.onSubscribe(new RangeSubscription(s, array, start, end));
        }
    }

    static abstract class AbstractRangeSubscription<T> extends AtomicLong implements FusedSubscription<T> {

        final T[] array;

        final int end;

        int index;

        volatile boolean cancelled;

        AbstractRangeSubscription(T[] array, int start, int end) {
            this.array = array;
            this.index = start;
            this.end = end;
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final T poll() throws Throwable {
            int idx = index;
            if (idx == end) {
                return null;
            }
            index = idx + 1;
            T v = array[idx];
            if (v == null) {
                throw new NullPointerException("Item " + idx + " is null");
            }
            return v;
        }

        @Override
        public final boolean isEmpty() {
            return index == end;
        }

        @Override
        public final void clear() {
            index = end;
        }

        @Override
        public final void request(long n) {
            if (SubscriptionHelper.addRequested(this, n) == 0) {
                if (n == Long.MAX_VALUE) {
                    fastPath();
                } else {
                    slowPath(n);
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        abstract void fastPath();

        abstract void slowPath(long n);
    }

    static final class RangeSubscription<T> extends AbstractRangeSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        RangeSubscription(FolyamSubscriber<? super T> actual, T[] array, int start, int end) {
            super(array, start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super T> a = actual;
            T[] items = array;
            int e = end;
            for (int i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                T v = items[i];
                if (v == null) {
                    a.onError(new NullPointerException("Item " + i + " is null"));
                    return;
                }
                a.onNext(v);
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super T> a = actual;
            T[] items = array;
            int idx = index;
            long e = 0L;
            int f = end;
            for (;;) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v = items[idx];
                    if (v == null) {
                        a.onError(new NullPointerException("Item " + idx + " is null"));
                        return;
                    }
                    a.onNext(v);

                    idx++;
                    e++;
                }

                if (idx == f) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }

                n = getAcquire();
                if (e == n) {
                    index = idx;
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0;
                }
            }
        }
    }

    static final class RangeConditionalSubscription<T> extends AbstractRangeSubscription<T> {

        final ConditionalSubscriber<? super T> actual;

        RangeConditionalSubscription(ConditionalSubscriber<? super T> actual, T[] array, int start, int end) {
            super(array, start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super T> a = actual;
            T[] items = array;
            int e = end;
            for (int i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                T v = items[i];
                if (v == null) {
                    a.onError(new NullPointerException("Item " + i + " is null"));
                    return;
                }
                a.tryOnNext(v);
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super T> a = actual;
            T[] items = array;
            int idx = index;
            long e = 0L;
            int f = end;
            for (; ; ) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v = items[idx];
                    if (v == null) {
                        a.onError(new NullPointerException("Item " + idx + " is null"));
                        return;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }
                    idx++;
                }

                if (idx == f) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }

                n = getAcquire();
                if (e == n) {
                    index = idx;
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0;
                }
            }
        }
    }
}
