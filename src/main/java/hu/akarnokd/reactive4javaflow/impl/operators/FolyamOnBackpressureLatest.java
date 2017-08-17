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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamOnBackpressureLatest<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedConsumer<? super T> onDrop;

    public FolyamOnBackpressureLatest(Folyam<T> source, CheckedConsumer<? super T> onDrop) {
        this.source = source;
        this.onDrop = onDrop;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnBackpressureLatestConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, onDrop));
        } else {
            source.subscribe(new OnBackpressureLatestSubscriber<>(s, onDrop));
        }
    }

    static abstract class AbstractOnBackpressureLatest<T> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription {

        final CheckedConsumer<? super T> onDrop;

        Flow.Subscription upstream;

        T value;
        static final VarHandle VALUE = VH.find(MethodHandles.lookup(), AbstractOnBackpressureLatest.class, "value", Object.class);

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractOnBackpressureLatest.class, "done", boolean.class);

        Throwable error;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractOnBackpressureLatest.class, "requested", long.class);

        long emitted;

        protected AbstractOnBackpressureLatest(CheckedConsumer<? super T> onDrop) {
            this.onDrop = onDrop;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;
            onStart();
            subscription.request(Long.MAX_VALUE);
        }

        abstract void onStart();

        final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        abstract void drainLoop();

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
            if (getAndIncrement() == 0) {
                VALUE.setRelease(this, null);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public final void onNext(T item) {
            if (done) {
                return;
            }
            T v = (T)VALUE.getAndSet(this, item);
            if (v != null) {
                try {
                    onDrop.accept(v);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    upstream.cancel();
                    onError(ex);
                    return;
                }
            }
            drain();
        }

        @Override
        public final void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            error = throwable;
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }
    }

    static final class OnBackpressureLatestSubscriber<T> extends AbstractOnBackpressureLatest<T> {

        final FolyamSubscriber<? super T> actual;

        protected OnBackpressureLatestSubscriber(FolyamSubscriber<? super T> actual, CheckedConsumer<? super T> onDrop) {
            super(onDrop);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            long e = emitted;

            for (;;) {
                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        VALUE.setRelease(this, null);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v = (T)VALUE.getAcquire(this);
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (empty || e == r) {
                        break;
                    }

                    v = (T)VALUE.getAndSet(this, null);

                    a.onNext(v);

                    e++;
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

    }

    static final class OnBackpressureLatestConditionalSubscriber<T> extends AbstractOnBackpressureLatest<T> {

        final ConditionalSubscriber<? super T> actual;

        protected OnBackpressureLatestConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedConsumer<? super T> onDrop) {
            super(onDrop);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super T> a = actual;
            long e = emitted;

            for (;;) {
                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        VALUE.setRelease(this, null);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v = (T)VALUE.getAcquire(this);
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (empty || e == r) {
                        break;
                    }

                    v = (T)VALUE.getAndSet(this, null);

                    if (a.tryOnNext(v)) {
                        e++;
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

    }
}
