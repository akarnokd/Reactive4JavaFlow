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
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.concurrent.atomic.AtomicLong;

public final class FolyamRepeatItem<T> extends Folyam<T> {

    final T item;

    public FolyamRepeatItem(T item) {
        this.item = item;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new RepeatItemConditionalSubscription<>((ConditionalSubscriber<? super T>)s, item));
        } else {
            s.onSubscribe(new RepeatItemSubscription<>(s, item));
        }
    }

    static abstract class AbstractRepeatItemSubscription<T> extends AtomicLong implements FusedSubscription<T> {

        T item;

        volatile boolean cancelled;

        AbstractRepeatItemSubscription(T item) {
            this.item = item;
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final T poll() throws Throwable {
            return item;
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

    static final class RepeatItemSubscription<T> extends AbstractRepeatItemSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        RepeatItemSubscription(FolyamSubscriber<? super T> actual, T item) {
            super(item);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super T> a = actual;
            T item = this.item;
            for (;;) {
                if (cancelled) {
                    return;
                }
                a.onNext(item);
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super T> a = actual;
            T item = this.item;
            long e = 0L;
            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    a.onNext(item);

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

    static final class RepeatItemConditionalSubscription<T> extends AbstractRepeatItemSubscription<T> {

        final ConditionalSubscriber<? super T> actual;

        RepeatItemConditionalSubscription(ConditionalSubscriber<? super T> actual, T item) {
            super(item);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super T> a = actual;
            T item = this.item;
            for (;;) {
                if (cancelled) {
                    return;
                }
                a.tryOnNext(item);
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super T> a = actual;
            long e = 0L;
            T item = this.item;
            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    if (a.tryOnNext(item)) {
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
