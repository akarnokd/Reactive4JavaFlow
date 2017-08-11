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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamConcatMapEager<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int maxConcurrency;

    final int prefetch;

    final boolean delayError;

    public FolyamConcatMapEager(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayError) {
        this.source = source;
        this.mapper = mapper;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        source.subscribe(createSubscriber(s, mapper, maxConcurrency, prefetch, delayError));
    }

    public static <T, R> FolyamSubscriber<T> createSubscriber(FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
        if (s instanceof ConditionalSubscriber) {
            return new ConcatMapEagerConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, maxConcurrency, prefetch, delayErrors);
        }
        return new ConcatMapEagerSubscriber<>(s, mapper, maxConcurrency, prefetch, delayErrors);
    }

    static abstract class AbstractConcatMapEager<T, R> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, QueuedFolyamSubscriberSupport<R> {

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        final int maxConcurrency;

        final int prefetch;

        final boolean delayError;

        Flow.Subscription upstream;

        QueuedInnerFolyamSubscriber<R>[] subscribers;
        static final VarHandle SUBSCRIBERS;

        static final QueuedInnerFolyamSubscriber[] EMPTY = new QueuedInnerFolyamSubscriber[0];
        static final QueuedInnerFolyamSubscriber[] TERMINATED = new QueuedInnerFolyamSubscriber[0];

        long requested;
        static final VarHandle REQUESTED;

        volatile boolean cancelled;

        QueuedInnerFolyamSubscriber<R> active;

        boolean done;
        static final VarHandle DONE;

        Throwable error;
        static final VarHandle ERROR;

        long emitted;

        static {
            Lookup lk = lookup();
            try {
                SUBSCRIBERS = lk.findVarHandle(AbstractConcatMapEager.class, "subscribers", QueuedInnerFolyamSubscriber[].class);
                REQUESTED = lk.findVarHandle(AbstractConcatMapEager.class, "requested", long.class);
                DONE = lk.findVarHandle(AbstractConcatMapEager.class, "done", boolean.class);
                ERROR = lk.findVarHandle(AbstractConcatMapEager.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractConcatMapEager(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayError) {
            this.mapper = mapper;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
            this.delayError = delayError;
            SUBSCRIBERS.setRelease(this, EMPTY);
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
            cancelAll();
            if (getAndIncrement() == 0) {
                active = null;
            }
        }

        final void cancelAll() {
            for (QueuedInnerFolyamSubscriber<?> inner : (QueuedInnerFolyamSubscriber<?>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                inner.cancel();
            }
        }


        @SuppressWarnings("unchecked")
        boolean add(QueuedInnerFolyamSubscriber<R> inner) {
            for (;;) {
                QueuedInnerFolyamSubscriber<R>[] a = (QueuedInnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                if (a == TERMINATED) {
                    return false;
                }
                int n = a.length;
                QueuedInnerFolyamSubscriber<R>[] b = new QueuedInnerFolyamSubscriber[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return true;
                }
            }
        }

        @SuppressWarnings("unchecked")
        void remove(QueuedInnerFolyamSubscriber<R> inner) {
            for (;;) {
                QueuedInnerFolyamSubscriber<R>[] a = (QueuedInnerFolyamSubscriber<R>[]) SUBSCRIBERS.getAcquire(this);
                int n = a.length;
                if (n == 0) {
                    break;
                }
                int j = -1;

                for (int i = 0; i < n; i++) {
                    if (inner == a[i]) {
                        j = i;
                        break;
                    }
                }

                if (j < 0) {
                    break;
                }
                QueuedInnerFolyamSubscriber<R>[] b;
                if (n == 1) {
                    b = EMPTY;
                } else {
                    b = new QueuedInnerFolyamSubscriber[n - 1];
                    System.arraycopy(a, 0, b, 0, j);
                    System.arraycopy(a, j + 1, b, j, n - j - 1);
                }
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    break;
                }
            }
        }


        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            int mc = maxConcurrency;
            if (mc == Integer.MAX_VALUE) {
                subscription.request(Long.MAX_VALUE);
            } else {
                subscription.request(mc);
            }
        }

        abstract void onStart();

        @Override
        public final void onNext(T item) {
            if (!done) {
                Flow.Publisher<? extends R> p;

                try {
                    p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Flow.Publisher");
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }

                QueuedInnerFolyamSubscriber<R> inner = new QueuedInnerFolyamSubscriber<>(this, 0, prefetch);
                if (add(inner)) {
                    p.subscribe(inner);
                }
            }
        }

        @Override
        public final void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                if (!delayError) {
                    cancelAll();
                }
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void innerError(QueuedInnerFolyamSubscriber<R> sender, int index, Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                if (!delayError) {
                    upstream.cancel();
                    cancelAll();
                }
                sender.setDone();
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        abstract void drainLoop();
    }

    static final class ConcatMapEagerSubscriber<T, R> extends AbstractConcatMapEager<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected ConcatMapEagerSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayError) {
            super(mapper, maxConcurrency, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            long e = emitted;
            QueuedInnerFolyamSubscriber<R> current = active;

            outer:
            for (;;) {

                if (current == null) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R>[] subs = (QueuedInnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                    boolean empty = subs.length == 0;

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (!empty) {
                        current = subs[0];
                        active = current;
                    }
                }

                if (current != null) {
                    long r = (long)REQUESTED.getAcquire(this);

                    while (e != r) {
                        if (cancelled) {
                            active = null;
                            return;
                        }
                        if (!delayError && ERROR.getAcquire(this) != null) {
                            active = null;
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            a.onError(ex);
                            return;
                        }

                        boolean d = current.isDone();
                        FusedQueue<R> q = current.getQueue();
                        R v;

                        try {
                            v = q != null ? q.poll() : null;
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            if (!delayError) {
                                active = null;
                                upstream.cancel();
                                cancelAll();
                                ex = ExceptionHelper.terminate(this, ERROR);
                                a.onError(ex);
                                return;
                            }
                            current.setDone();
                            d = true;
                            v = null;
                        }

                        boolean empty = v == null;

                        if (d && empty) {
                            remove(current);
                            current = null;
                            active = null;
                            upstream.request(1);
                            continue outer;
                        }

                        if (empty) {
                            break;
                        }

                        a.onNext(v);

                        e++;

                        current.request();
                    }

                    if (e == r) {
                        if (cancelled) {
                            active = null;
                            return;
                        }

                        if (!delayError && ERROR.getAcquire(this) != null) {
                            active = null;
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            a.onError(ex);
                            return;
                        }

                        if (current.isDone() && current.getQueue().isEmpty()) {
                            remove(current);
                            current = null;
                            active = null;
                            upstream.request(1);
                            continue;
                        }
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


    static final class ConcatMapEagerConditionalSubscriber<T, R> extends AbstractConcatMapEager<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected ConcatMapEagerConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayError) {
            super(mapper, maxConcurrency, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super R> a = actual;
            long e = emitted;
            QueuedInnerFolyamSubscriber<R> current = active;

            outer:
            for (;;) {

                if (current == null) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R>[] subs = (QueuedInnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                    boolean empty = subs.length == 0;

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (!empty) {
                        current = subs[0];
                        active = current;
                    }
                }

                if (current != null) {
                    long r = (long)REQUESTED.getAcquire(this);

                    while (e != r) {
                        if (cancelled) {
                            active = null;
                            return;
                        }
                        if (!delayError && ERROR.getAcquire(this) != null) {
                            active = null;
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            a.onError(ex);
                            return;
                        }

                        boolean d = current.isDone();
                        FusedQueue<R> q = current.getQueue();
                        R v;

                        try {
                            v = q != null ? q.poll() : null;
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            if (!delayError) {
                                active = null;
                                upstream.cancel();
                                cancelAll();
                                ex = ExceptionHelper.terminate(this, ERROR);
                                a.onError(ex);
                                return;
                            }
                            current.setDone();
                            d = true;
                            v = null;
                        }

                        boolean empty = v == null;

                        if (d && empty) {
                            remove(current);
                            current = null;
                            active = null;
                            upstream.request(1);
                            continue outer;
                        }

                        if (empty) {
                            break;
                        }

                        if (a.tryOnNext(v)) {
                            e++;
                        }

                        current.request();
                    }

                    if (e == r) {
                        if (cancelled) {
                            active = null;
                            return;
                        }

                        if (!delayError && ERROR.getAcquire(this) != null) {
                            active = null;
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            a.onError(ex);
                            return;
                        }

                        if (current.isDone() && current.getQueue().isEmpty()) {
                            remove(current);
                            current = null;
                            active = null;
                            upstream.request(1);
                            continue;
                        }
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
