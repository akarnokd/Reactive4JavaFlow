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
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

public final class FolyamCombineLatest<T, R> extends Folyam<R> {

    final Flow.Publisher<? extends T>[] sources;

    final CheckedFunction<? super Object[], ? extends R> combiner;

    final int prefetch;

    final boolean delayError;

    public FolyamCombineLatest(Flow.Publisher<? extends T>[] sources, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch, boolean delayError) {
        this.sources = sources;
        this.combiner = combiner;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        subscribe(sources, sources.length, s, combiner, prefetch, delayError);
    }

    public static <T, R> void subscribe(Flow.Publisher<? extends T>[] sources, int n, FolyamSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch, boolean delayError) {

        if (n == 0) {
            EmptySubscription.complete(actual);
            return;
        }
        if (actual instanceof ConditionalSubscriber) {
            if (n == 1) {
                sources[0].subscribe(new FolyamMap.MapConditionalSubscriber<T, R>((ConditionalSubscriber<? super R>) actual, v -> combiner.apply(new Object[] { v })));
            } else {
                CombineLatestConditionalCoordinator<T, R> parent = new CombineLatestConditionalCoordinator<>((ConditionalSubscriber<? super R>) actual, combiner, n, prefetch, delayError);
                actual.onSubscribe(parent);
                parent.subscribe(sources, n);
            }
        } else {
            if (n == 1) {
                sources[0].subscribe(new FolyamMap.MapSubscriber<>(actual, v -> combiner.apply(new Object[]{ v })));
            } else {
                CombineLatestCoordinator<T, R> parent = new CombineLatestCoordinator<>(actual, combiner, n, prefetch, delayError);
                actual.onSubscribe(parent);
                parent.subscribe(sources, n);
            }
        }
    }

    static abstract class AbstractCombineLatest<T, R> extends AtomicInteger implements FusedSubscription<R> {

        final CheckedFunction<? super Object[], ? extends R> combiner;

        final boolean delayError;

        final CombineLatestInnerSubscriber<T>[] subscribers;

        final SpscLinkedArrayQueue<Object> queue;

        boolean outputFused;

        volatile boolean cancelled;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractCombineLatest.class, "requested", Long.TYPE);

        Object[] values;
        static final VarHandle VALUES = VH.find(MethodHandles.lookup(), AbstractCombineLatest.class, "values", Object[].class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractCombineLatest.class, "error", Throwable.class);

        long emitted;

        int done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractCombineLatest.class, "done", Integer.TYPE);

        int active;


        protected AbstractCombineLatest(CheckedFunction<? super Object[], ? extends R> combiner, int n, int prefetch, boolean delayError) {
            this.combiner = combiner;
            this.delayError = delayError;
            CombineLatestInnerSubscriber<T>[] subs = new CombineLatestInnerSubscriber[n];
            for (int i = 0; i < n; i++) {
                subs[i] = new CombineLatestInnerSubscriber<>(this, i, prefetch);
            }
            this.subscribers = subs;
            this.queue = new SpscLinkedArrayQueue<>(prefetch);
            VALUES.setRelease(this, new Object[n]);
        }

        final void subscribe(Flow.Publisher<? extends T>[] sources, int n) {
            CombineLatestInnerSubscriber<T>[] subs = this.subscribers;

            for (int i = 0; i < n; i++) {
                if (cancelled || (!delayError && ERROR.getAcquire(this) != null)) {
                    return;
                }
                Flow.Publisher<? extends T> p = sources[i];
                if (p == null) {
                    EmptySubscription.error(subs[i], new NullPointerException("Flow.Publisher[" + i + "] == null"));
                    for (int j = i + 1; j < n; j++) {
                        EmptySubscription.complete(subs[i]);
                    }
                    break;
                } else {
                    p.subscribe(subs[i]);
                }
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
            cancelSubscribers(this.subscribers);
            if (getAndIncrement() == 0) {
                VALUES.setRelease(this, null);
                queue.clear();
            }
        }

        final void cancelSubscribers(CombineLatestInnerSubscriber<T>[] subs) {
            for (CombineLatestInnerSubscriber<?> inner : subs) {
                inner.cancel();
            }
        }

        final void innerNext(CombineLatestInnerSubscriber<T> sender, int index, T item) {
            boolean shouldDrain = false;
            Object[] vals = (Object[])VALUES.getAcquire(this);
            if (vals == null) {
                return;
            }

            synchronized (this) {
                int a = active;

                if (vals[index] == null) {
                    a++;
                    active = a;
                }
                vals[index] = item;
                if (a == vals.length) {
                    shouldDrain = true;
                    queue.offer(vals.clone(), sender);
                }
            }

            if (shouldDrain) {
                drain();
            } else {
                sender.request();
            }
        }

        final void innerError(CombineLatestInnerSubscriber<T> sender, Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                if (!delayError) {
                    cancelSubscribers(subscribers);
                } else {
                    DONE.getAndAdd(this, 1);
                }
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        final void innerComplete(CombineLatestInnerSubscriber<T> sender) {
            DONE.getAndAdd(this, 1);
            drain();
        }

        @Override
        public int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public R poll() throws Throwable {
            SpscLinkedArrayQueue<Object> q = this.queue;
            Object[] vals = (Object[]) q.poll();
            if (vals != null) {
                CombineLatestInnerSubscriber<?> sender = (CombineLatestInnerSubscriber<?>) q.poll();

                R v = Objects.requireNonNull(combiner.apply(vals), "The combiner returned a null value");

                sender.request();

                return v;
            }
            return null;
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            VALUES.setRelease(this, null);
            queue.clear();
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                if (outputFused) {
                    drainFusedLoop();
                } else {
                    drainLoop();
                }
            }
        }

        abstract void drainFusedLoop();

        abstract void drainLoop();
    }

    static final class CombineLatestCoordinator<T, R> extends AbstractCombineLatest<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected CombineLatestCoordinator(FolyamSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> combiner, int n, int prefetch, boolean delayError) {
            super(combiner, n, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void drainFusedLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            SpscLinkedArrayQueue<Object> q = queue;
            int n = subscribers.length;

            for (; ; ) {
                if (cancelled) {
                    VALUES.setRelease(this, null);
                    q.clear();
                    return;
                }

                if (!delayError) {
                    Throwable ex = (Throwable) ERROR.getAcquire(this);
                    if (ex != null) {
                        ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        q.clear();
                        a.onError(ex);
                        return;
                    }
                }

                boolean d = n == (int) DONE.getAcquire(this);

                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    VALUES.setRelease(this, null);
                    if (ex != null) {
                        a.onError(ex);
                    } else {
                        a.onComplete();
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
            SpscLinkedArrayQueue<Object> q = queue;
            CheckedFunction<? super Object[], ? extends R> combiner = this.combiner;
            long e = emitted;
            int n = subscribers.length;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        VALUES.setRelease(this, null);
                        q.clear();
                        return;
                    }

                    if (!delayError) {
                        Throwable ex = (Throwable)ERROR.getAcquire(this);
                        if (ex != null) {
                            ex = ExceptionHelper.terminate(this, ERROR);
                            VALUES.setRelease(this, null);
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                    }

                    boolean d = n == (int)DONE.getAcquire(this);

                    boolean empty = q.isEmpty();

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (e == r || empty) {
                        break;
                    }

                    Object[] array = (Object[])q.poll();
                    CombineLatestInnerSubscriber<?> sender = (CombineLatestInnerSubscriber<?>)q.poll();

                    R v;

                    try {
                        v = Objects.requireNonNull(combiner.apply(array), "The combiner returned a null value");
                    } catch (Throwable ex) {
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        cancelSubscribers(subscribers);
                        ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        q.clear();
                        a.onError(ex);
                        return;

                    }

                    a.onNext(v);

                    e++;

                    sender.request();
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class CombineLatestConditionalCoordinator<T, R> extends AbstractCombineLatest<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected CombineLatestConditionalCoordinator(ConditionalSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> combiner, int n, int prefetch, boolean delayError) {
            super(combiner, n, prefetch, delayError);
            this.actual = actual;
        }


        @Override
        void drainFusedLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            SpscLinkedArrayQueue<Object> q = queue;
            int n = subscribers.length;

            for (; ; ) {
                if (cancelled) {
                    VALUES.setRelease(this, null);
                    q.clear();
                    return;
                }

                if (!delayError) {
                    Throwable ex = (Throwable) ERROR.getAcquire(this);
                    if (ex != null) {
                        ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        q.clear();
                        a.onError(ex);
                        return;
                    }
                }

                boolean d = n == (int) DONE.getAcquire(this);

                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    VALUES.setRelease(this, null);
                    if (ex != null) {
                        a.onError(ex);
                    } else {
                        a.onComplete();
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
            SpscLinkedArrayQueue<Object> q = queue;
            CheckedFunction<? super Object[], ? extends R> combiner = this.combiner;
            long e = emitted;
            int n = subscribers.length;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        VALUES.setRelease(this, null);
                        q.clear();
                        return;
                    }

                    if (!delayError) {
                        Throwable ex = (Throwable)ERROR.getAcquire(this);
                        if (ex != null) {
                            ex = ExceptionHelper.terminate(this, ERROR);
                            VALUES.setRelease(this, null);
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                    }

                    boolean d = n == (int)DONE.getAcquire(this);

                    boolean empty = q.isEmpty();

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (e == r || empty) {
                        break;
                    }

                    Object[] array = (Object[])q.poll();
                    CombineLatestInnerSubscriber<?> sender = (CombineLatestInnerSubscriber<?>)q.poll();

                    R v;

                    try {
                        v = Objects.requireNonNull(combiner.apply(array), "The combiner returned a null value");
                    } catch (Throwable ex) {
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        cancelSubscribers(subscribers);
                        ex = ExceptionHelper.terminate(this, ERROR);
                        VALUES.setRelease(this, null);
                        q.clear();
                        a.onError(ex);
                        return;

                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    sender.request();
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class CombineLatestInnerSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<T> {

        final AbstractCombineLatest<T, ?> parent;

        final int index;

        final int prefetch;

        final int limit;

        int consumed;

        CombineLatestInnerSubscriber(AbstractCombineLatest<T, ?> parent, int index, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.index = index;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, subscription)) {
                subscription.request(prefetch);
            }
        }

        @Override
        public void onNext(T item) {
            parent.innerNext(this, index, item);
        }

        @Override
        public void onError(Throwable throwable) {
            parent.innerError(this, throwable);
        }

        @Override
        public void onComplete() {
            parent.innerComplete(this);
        }

        void cancel() {
            SubscriptionHelper.cancel(this);
        }

        void request() {
            int c = consumed + 1;
            if (c == limit) {
                consumed = 0;
                getPlain().request(c);
            } else {
                consumed = c;
            }
        }
    }
}
