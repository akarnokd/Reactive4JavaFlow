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
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamFlatMap<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int maxConcurrency;

    final int prefetch;

    final boolean delayErrors;

    public FolyamFlatMap(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
        this.source = source;
        this.mapper = mapper;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
        this.delayErrors = delayErrors;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        source.subscribe(createSubscriber(s, mapper, maxConcurrency, prefetch, delayErrors));
    }

    public static <T, R> FolyamSubscriber<T> createSubscriber(FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
        if (s instanceof ConditionalSubscriber) {
            return new FlatMapConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, maxConcurrency, prefetch, delayErrors);
        }
        return new FlatMapSubscriber<>(s, mapper, maxConcurrency, prefetch, delayErrors);
    }

    static abstract class AbstractFlatMap<T, R> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, InnerFolyamSubscriberSupport<R> {

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        final int maxConcurrency;

        final int prefetch;

        final boolean delayErrors;

        final int limit;

        Flow.Subscription upstream;

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractFlatMap.class, "done", Boolean.TYPE);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractFlatMap.class, "error", Throwable.class);

        PlainQueue<R> scalarQueue;
        static final VarHandle SCALAR_QUEUE = VH.find(MethodHandles.lookup(), AbstractFlatMap.class, "scalarQueue", PlainQueue.class);

        InnerFolyamSubscriber<R>[] subscribers;
        static final VarHandle SUBSCRIBERS = VH.find(MethodHandles.lookup(), AbstractFlatMap.class, "subscribers", InnerFolyamSubscriber[].class);

        static final InnerFolyamSubscriber[] EMPTY = new InnerFolyamSubscriber[0];
        static final InnerFolyamSubscriber[] TERMINATED = new InnerFolyamSubscriber[0];

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractFlatMap.class, "requested", Long.TYPE);

        int consumed;
        long emitted;

        protected AbstractFlatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
            this.mapper = mapper;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
            this.delayErrors = delayErrors;
            this.limit = maxConcurrency == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxConcurrency - (maxConcurrency >> 2);
            SUBSCRIBERS.setRelease(this, EMPTY);
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            subscription.request(maxConcurrency == Integer.MAX_VALUE ? Long.MAX_VALUE : maxConcurrency);
        }

        abstract void onStart();

        @SuppressWarnings("unchecked")
        boolean add(InnerFolyamSubscriber<R> inner) {
            for (;;) {
                InnerFolyamSubscriber<R>[] a = (InnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                if (a == TERMINATED) {
                    return false;
                }
                int n = a.length;
                InnerFolyamSubscriber<R>[] b = new InnerFolyamSubscriber[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return true;
                }
            }
        }

        @SuppressWarnings("unchecked")
        void remove(InnerFolyamSubscriber<R> inner) {
            for (;;) {
                InnerFolyamSubscriber<R>[] a = (InnerFolyamSubscriber<R>[]) SUBSCRIBERS.getAcquire(this);
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
                InnerFolyamSubscriber<R>[] b;
                if (n == 1) {
                    b = EMPTY;
                } else {
                    b = new InnerFolyamSubscriber[n - 1];
                    System.arraycopy(a, 0, b, 0, j);
                    System.arraycopy(a, j + 1, b, j, n - j - 1);
                }
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    break;
                }
            }
        }

        @Override
        public final void onNext(T item) {
            Flow.Publisher<? extends R> p;

            try {
                p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Flow.Publisher");
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return;
            }

            if (p instanceof FusedDynamicSource) {
                FusedDynamicSource<R> fs = (FusedDynamicSource<R>) p;
                R v;
                try {
                    v = fs.value();
                } catch (Throwable ex) {
                    scalarError(ex);
                    return;
                }
                scalarValue(v);
            } else {
                InnerFolyamSubscriber<R> inner = new InnerFolyamSubscriber<>(this, p instanceof Esetleg ? 1 : prefetch);
                if (add(inner)) {
                    p.subscribe(inner);
                }
            }
        }

        @Override
        public final void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        abstract void emitNext(R item, long e);

        final void consumedOne() {
            int c = consumed + 1;
            int lim = limit;
            if (c == lim) {
                consumed = 0;
                upstream.request(lim);
            } else {
                consumed = c;
            }
        }

        final void scalarValue(R item) {
            if (getAcquire() == 0 && compareAndSet(0, 1)) {
                if (item == null) {
                    consumedOne();
                } else {
                    long r = (long) REQUESTED.getAcquire(this);
                    long e = emitted;

                    PlainQueue<R> sq = (PlainQueue<R>) SCALAR_QUEUE.get(this);
                    if (e != r && (sq == null || sq.isEmpty())) {
                        emitNext(item, e);
                        consumedOne();
                    } else {
                        if (sq == null) {
                            sq = createScalarQueue();
                        }
                        sq.offer(item);
                    }
                }

                if (decrementAndGet() == 0) {
                    return;
                }
            } else {
                PlainQueue<R> sq = getOrCreateScalarQueue();
                sq.offer(item);
                if (getAndIncrement() != 0) {
                    return;
                }
            }
            drainLoop();
        }

        PlainQueue<R> getOrCreateScalarQueue() {
            PlainQueue<R> sq = (PlainQueue<R>)SCALAR_QUEUE.get(this);
            if (sq == null) {
                sq = createScalarQueue();
            }
            return sq;
        }

        PlainQueue<R> createScalarQueue() {
            PlainQueue<R> sq;
            int pf = prefetch;
            if (pf == 1) {
                sq = new SpscOneQueue<>();
            } else {
                sq = new SpscArrayQueue<>(pf);
            }
            SCALAR_QUEUE.setRelease(this, sq);
            return sq;
        }

        final void scalarError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                if (!delayErrors) {
                    upstream.cancel();
                    DONE.setRelease(this, true);
                }
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public final void innerNext(InnerFolyamSubscriber<R> sender, R item) {
            if (get() == 0 && compareAndSet(0, 1)) {
                FusedQueue<R> q = sender.getQueuePlain();
                long r = (long)REQUESTED.getAcquire(this);
                long e = emitted;

                if (e != r && (q == null || q.isEmpty())) {
                    emitNext(item, e);
                    sender.produced(1, limit);
                } else {
                    if (q == null) {
                        q = sender.createQueue();
                    }
                    q.offer(item);
                }

                if (decrementAndGet() == 0) {
                    return;
                }
            } else {
                FusedQueue<R> q = sender.getOrCreateQueue();
                q.offer(item);
                if (getAndIncrement() != 0) {
                    return;
                }
            }
            drainLoop();
        }

        @Override
        public final void innerError(InnerFolyamSubscriber<R> sender, Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                sender.setDone();
                if (!delayErrors) {
                    upstream.cancel();
                    DONE.setRelease(this, true);
                }
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public final void innerComplete(InnerFolyamSubscriber<R> sender) {
            sender.setDone();
            drain();
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
            cancelInners();
            if (getAndIncrement() == 0) {
                scalarQueue = null;
            }
        }

        final void cancelInners() {
            InnerFolyamSubscriber<T>[] a = (InnerFolyamSubscriber<T>[])SUBSCRIBERS.getAndSet(this, TERMINATED);
            for (InnerFolyamSubscriber<T> b : a) {
                b.close();
            }
        }

        @Override
        public final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        abstract void drainLoop();

        final boolean checkTerminated(FolyamSubscriber<?> a, boolean delayErrors) {
            if (cancelled) {
                scalarQueue = null;
                return true;
            }

            if (!delayErrors) {
                Throwable ex = (Throwable) ERROR.getAcquire(this);
                if (ex != null) {
                    ex = ExceptionHelper.terminate(this, ERROR);
                    scalarQueue = null;
                    cancelInners();
                    a.onError(ex);
                    return true;
                }
            }
            return false;
        }
    }

    static final class FlatMapSubscriber<T, R> extends AbstractFlatMap<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected FlatMapSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
            super(mapper, maxConcurrency, prefetch, delayErrors);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void emitNext(R item, long e) {
            actual.onNext(item);
            emitted = e + 1;
        }

        @Override
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            long e = emitted;
            boolean delayErrors = this.delayErrors;
            int lim = limit;
            int c = consumed;

            for (;;) {

                if (checkTerminated(a, delayErrors)) {
                    return;
                }

                boolean again = false;

                long r = (long)REQUESTED.getAcquire(this);

                boolean d = (boolean)DONE.getAcquire(this);
                PlainQueue<R> sq = (PlainQueue<R>)SCALAR_QUEUE.getAcquire(this);
                if (sq != null && !sq.isEmpty()) {
                    while (e != r) {
                        if (checkTerminated(a, delayErrors)) {
                            return;
                        }

                        R v = sq.poll();

                        if (v == null) {
                            again = true;
                            break;
                        }

                        a.onNext(v);

                        e++;

                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }

                    }
                }

                InnerFolyamSubscriber<R>[] inners = (InnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                int n = inners.length;

                if (d && n == 0 && (sq == null || sq.isEmpty())) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

                for (InnerFolyamSubscriber<R> inner : inners) {
                    if (checkTerminated(a, delayErrors)) {
                        return;
                    }

                    boolean di = inner.isDone();
                    FusedQueue<R> q = inner.getQueue();
                    boolean noqueue = q == null || q.isEmpty();

                    if (di && noqueue) {
                        again = true;
                        remove(inner);
                        upstream.request(1);
                    } else if (!noqueue) {
                        while (e != r) {
                            if (checkTerminated(a, delayErrors)) {
                                return;
                            }

                            di = inner.isDone();

                            R v;

                            try {
                                v = q.poll();
                            } catch (Throwable ex) {
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayErrors) {
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    scalarQueue = null;
                                    cancelInners();
                                    a.onError(ex);
                                    return;
                                }
                                di = true;
                                v = null;
                            }

                            boolean empty = v == null;

                            if (di && empty) {
                                again = true;
                                remove(inner);
                                upstream.request(1);
                                break;
                            }

                            if (empty) {
                                break;
                            }

                            a.onNext(v);

                            e++;

                            inner.produced(1, lim);
                        }

                        if (e == r) {
                            if (checkTerminated(a, delayErrors)) {
                                return;
                            }

                            if (inner.isDone() && q.isEmpty()) {
                                again = true;
                                remove(inner);
                                upstream.request(1);
                            }
                        }
                    }
                }

                if (!again) {
                    consumed = c;
                    emitted = e;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }
    }

    static final class FlatMapConditionalSubscriber<T, R> extends AbstractFlatMap<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected FlatMapConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
            super(mapper, maxConcurrency, prefetch, delayErrors);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void emitNext(R item, long e) {
            if (actual.tryOnNext(item)) {
                emitted = e + 1;
            }
        }

        @Override
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super R> a = actual;
            long e = emitted;
            boolean delayErrors = this.delayErrors;
            int lim = limit;
            int c = consumed;

            for (;;) {

                if (checkTerminated(a, delayErrors)) {
                    return;
                }

                boolean again = false;

                long r = (long)REQUESTED.getAcquire(this);

                boolean d = (boolean)DONE.getAcquire(this);
                PlainQueue<R> sq = (PlainQueue<R>)SCALAR_QUEUE.getAcquire(this);
                if (sq != null && !sq.isEmpty()) {
                    while (e != r) {
                        if (checkTerminated(a, delayErrors)) {
                            return;
                        }

                        R v = sq.poll();

                        if (v == null) {
                            again = true;
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
                }

                InnerFolyamSubscriber<R>[] inners = (InnerFolyamSubscriber<R>[])SUBSCRIBERS.getAcquire(this);
                int n = inners.length;

                if (d && n == 0 && (sq == null || sq.isEmpty())) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

                for (InnerFolyamSubscriber<R> inner : inners) {
                    if (checkTerminated(a, delayErrors)) {
                        return;
                    }

                    boolean di = inner.isDone();
                    FusedQueue<R> q = inner.getQueue();
                    boolean noqueue = q == null || q.isEmpty();

                    if (di && noqueue) {
                        again = true;
                        remove(inner);
                        upstream.request(1);
                    } else if (!noqueue) {
                        while (e != r) {
                            if (checkTerminated(a, delayErrors)) {
                                return;
                            }

                            di = inner.isDone();

                            R v;

                            try {
                                v = q.poll();
                            } catch (Throwable ex) {
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayErrors) {
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    scalarQueue = null;
                                    cancelInners();
                                    a.onError(ex);
                                    return;
                                }
                                di = true;
                                v = null;
                            }

                            boolean empty = v == null;

                            if (di && empty) {
                                again = true;
                                remove(inner);
                                upstream.request(1);
                                break;
                            }

                            if (empty) {
                                break;
                            }

                            if (a.tryOnNext(v)) {
                                e++;
                            }

                            inner.produced(1, lim);
                        }

                        if (e == r) {
                            if (checkTerminated(a, delayErrors)) {
                                return;
                            }

                            if (inner.isDone() && q.isEmpty()) {
                                again = true;
                                remove(inner);
                                upstream.request(1);
                            }
                        }
                    }
                }

                if (!again) {
                    consumed = c;
                    emitted = e;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }
    }
}
