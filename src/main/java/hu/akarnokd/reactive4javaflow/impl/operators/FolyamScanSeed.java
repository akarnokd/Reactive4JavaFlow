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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiFunction;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamScanSeed<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final Callable<? extends R> initialSupplier;

    final CheckedBiFunction<R, ? super T, R> scanner;

    final int prefetch;

    public FolyamScanSeed(Folyam<T> source, Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> scanner, int prefetch) {
        this.source = source;
        this.initialSupplier = initialSupplier;
        this.scanner = scanner;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        R initial;

        try {
            initial = Objects.requireNonNull(initialSupplier.call(), "The initialSupplier returned a null value");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new ScanSeedConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, scanner, prefetch, initial));
        } else {
            source.subscribe(new ScanSeedSubscriber<>(s, scanner, prefetch, initial));
        }
    }

    static abstract class AbstractScanSeed<T, R> extends AtomicInteger implements FolyamSubscriber<T>, FusedSubscription<R> {

        final CheckedBiFunction<R, ? super T, R> scanner;

        final int prefetch;

        final int limit;

        final PlainQueue<R> queue;

        Flow.Subscription upstream;

        boolean outputFused;

        R accumulator;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractScanSeed.class, "requested", long.class);

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractScanSeed.class, "done", boolean.class);

        Throwable error;

        long emitted;

        int consumed;

        protected AbstractScanSeed(CheckedBiFunction<R, ? super T, R> scanner, int prefetch, R initialValue) {
            this.scanner = scanner;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.queue = new SpscArrayQueue<>(prefetch);
            accumulator = initialValue;
            queue.offer(initialValue);
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            int n = prefetch - 1;
            if (n > 0) {
                subscription.request(n);
            }
        }

        abstract void onStart();

        @Override
        public final void onNext(T item) {
            R a = accumulator;
            if (a != null) {
                try {
                    a = Objects.requireNonNull(scanner.apply(a, item), "The scanner returned a null value");
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
                accumulator = a;
                queue.offer(a);
                drain();
            }
        }

        @Override
        public final void onError(Throwable throwable) {
            if (accumulator != null) {
                accumulator = null;
                error = throwable;
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public final void onComplete() {
            if (accumulator != null) {
                accumulator = null;
                DONE.setRelease(this, true);
                drain();
            }
        }

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
                accumulator = null;
                queue.clear();
            }
        }

        @Override
        public final int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public final R poll() throws Throwable {
            R v = queue.poll();
            if (v != null && outputFused) {
                int c = consumed + 1;
                if (c == limit) {
                    consumed = 0;
                    upstream.request(c);
                } else {
                    consumed = c;
                }
            }
            return v;
        }

        @Override
        public final boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public final void clear() {
            accumulator = null;
            queue.clear();
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                if (outputFused) {
                    drainFused();
                } else {
                    drainLoop();
                }
            }
        }

        abstract void drainFused();

        abstract void drainLoop();
    }

    static final class ScanSeedSubscriber<T, R> extends AbstractScanSeed<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected ScanSeedSubscriber(FolyamSubscriber<? super R> actual, CheckedBiFunction<R, ? super T, R> scanner, int prefetch, R initialValue) {
            super(scanner, prefetch, initialValue);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainFused() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            PlainQueue<R> q = queue;

            for (;;) {

                if (cancelled) {
                    accumulator = null;
                    queue.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            PlainQueue<R> q = queue;
            long e = emitted;
            int c = consumed;
            int lim = limit;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        accumulator = null;
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    R v = q.poll();
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

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;

                    if (++c == lim) {
                        c = 0;
                        upstream.request(lim);
                    }

                }

                if (e == r) {
                    if (cancelled) {
                        accumulator = null;
                        q.clear();
                        return;
                    }

                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                }

                emitted = e;
                consumed = c;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }


    static final class ScanSeedConditionalSubscriber<T, R> extends AbstractScanSeed<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected ScanSeedConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedBiFunction<R, ? super T, R> scanner, int prefetch, R initialValue) {
            super(scanner, prefetch, initialValue);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainFused() {
            int missed = 1;
            ConditionalSubscriber<? super R> a = actual;
            PlainQueue<R> q = queue;

            for (;;) {

                if (cancelled) {
                    accumulator = null;
                    queue.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                if (!q.isEmpty()) {
                    a.tryOnNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super R> a = actual;
            PlainQueue<R> q = queue;
            long e = emitted;
            int c = consumed;
            int lim = limit;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        accumulator = null;
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    R v = q.poll();
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

                    if (empty) {
                        break;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    if (++c == lim) {
                        c = 0;
                        upstream.request(lim);
                    }

                }

                if (e == r) {
                    if (cancelled) {
                        accumulator = null;
                        q.clear();
                        return;
                    }

                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                }

                emitted = e;
                consumed = c;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
