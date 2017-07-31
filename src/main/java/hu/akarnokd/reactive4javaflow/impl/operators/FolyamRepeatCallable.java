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

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamRepeatCallable<T> extends Folyam<T> {

    final Callable<? extends T> item;

    public FolyamRepeatCallable(Callable<? extends T> item) {
        this.item = item;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new RepeatCallableConditionalSubscription<>((ConditionalSubscriber<? super T>)s, item));
        } else {
            s.onSubscribe(new RepeatCallableSubscription<>(s, item));
        }
    }

    static abstract class AbstractRepeatCallableSubscription<T> extends AtomicLong implements FusedSubscription<T> {

        Callable<? extends T> item;

        volatile boolean cancelled;

        AbstractRepeatCallableSubscription(Callable<? extends T> item) {
            this.item = item;
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final T poll() throws Throwable {
            Callable<? extends T> item = this.item;
            if (item == null) {
                return null;
            }
            return Objects.requireNonNull(item.call(), "The callable returned a null item");
        }

        @Override
        public final boolean isEmpty() {
            return item == null;
        }

        @Override
        public final void clear() {
            item = null;
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

    static final class RepeatCallableSubscription<T> extends AbstractRepeatCallableSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        RepeatCallableSubscription(FolyamSubscriber<? super T> actual, Callable<? extends T> item) {
            super(item);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super T> a = actual;
            Callable<? extends T> item = this.item;
            for (;;) {
                if (cancelled) {
                    return;
                }

                T v;

                try {
                    v = Objects.requireNonNull(item.call(), "The callable returned a null item.");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }
                a.onNext(v);
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super T> a = actual;
            Callable<? extends T> item = this.item;
            long e = 0L;
            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v;

                    try {
                        v = Objects.requireNonNull(item.call(), "The callable returned a null item.");
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        return;
                    }
                    a.onNext(v);

                    e++;
                }

                n = getAcquire();
                if (e == n) {
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0;
                }
            }
        }
    }

    static final class RepeatCallableConditionalSubscription<T> extends AbstractRepeatCallableSubscription<T> {

        final ConditionalSubscriber<? super T> actual;

        RepeatCallableConditionalSubscription(ConditionalSubscriber<? super T> actual, Callable<? extends T> item) {
            super(item);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super T> a = actual;
            Callable<? extends T> item = this.item;
            for (;;) {
                if (cancelled) {
                    return;
                }
                T v;

                try {
                    v = Objects.requireNonNull(item.call(), "The callable returned a null item.");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }
                a.tryOnNext(v);
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super T> a = actual;
            long e = 0L;
            Callable<? extends T> item = this.item;
            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v;

                    try {
                        v = Objects.requireNonNull(item.call(), "The callable returned a null item.");
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        return;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }
                }

                n = getAcquire();
                if (e == n) {
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
