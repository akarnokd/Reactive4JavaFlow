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
import hu.akarnokd.reactive4javaflow.fused.FusedQueue;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

/**
 * Merges the individual 'rails' of the source ParallelFolyam, unordered,
 * into a single regular Publisher sequence (exposed as Folyam).
 *
 * @param <T> the value type
 */
public final class ParallelJoinAsync<T> extends Folyam<T> {

    final ParallelFolyam<? extends T> source;

    final int prefetch;

    final boolean delayErrors;

    final SchedulerService executor;

    public ParallelJoinAsync(ParallelFolyam<? extends T> source, int prefetch, boolean delayErrors, SchedulerService executor) {
        this.source = source;
        this.prefetch = prefetch;
        this.delayErrors = delayErrors;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        JoinSubscriptionBase<T> parent;
        if (delayErrors) {
            parent = new JoinSubscriptionDelayError<>(s, source.parallelism(), prefetch, executor.worker());
        } else {
            parent = new JoinSubscription<>(s, source.parallelism(), prefetch, executor.worker());
        }
        s.onSubscribe(parent);
        source.subscribe(parent.subscribers);
    }

    abstract static class JoinSubscriptionBase<T> extends AtomicInteger
    implements Flow.Subscription, Runnable {

        private static final long serialVersionUID = 3100232009247827843L;

        final FolyamSubscriber<? super T> actual;

        final JoinInnerSubscriber<T>[] subscribers;

        final SchedulerService.Worker worker;

        final AtomicReference<Throwable> errors = new AtomicReference<>();

        final AtomicLong requested = new AtomicLong();

        volatile boolean cancelled;

        final AtomicInteger done = new AtomicInteger();

        JoinSubscriptionBase(FolyamSubscriber<? super T> actual, int n, int prefetch, SchedulerService.Worker worker) {
            this.actual = actual;
            @SuppressWarnings("unchecked")
            JoinInnerSubscriber<T>[] a = new JoinInnerSubscriber[n];

            for (int i = 0; i < n; i++) {
                a[i] = new JoinInnerSubscriber<>(this, prefetch);
            }

            this.subscribers = a;
            this.worker = worker;
            done.lazySet(n);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(requested, n);
            drain();
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;

                cancelAll();

                worker.close();
                if (getAndIncrement() == 0) {
                    cleanup();
                }
            }
        }

        void cancelAll() {
            for (int i = 0; i < subscribers.length; i++) {
                JoinInnerSubscriber<T> s = subscribers[i];
                s.cancel();
            }
        }

        void cleanup() {
            for (int i = 0; i < subscribers.length; i++) {
                JoinInnerSubscriber<T> s = subscribers[i];
                s.queue = null;
            }
        }

        abstract void onNext(JoinInnerSubscriber<T> inner, T value);

        abstract void onError(Throwable e);

        abstract void onComplete();

        final void drain() {
            if (getAndIncrement() == 0) {
                worker.schedule(this);
            }
        }
    }

    static final class JoinSubscription<T> extends JoinSubscriptionBase<T> {

        private static final long serialVersionUID = 6312374661811000451L;

        JoinSubscription(FolyamSubscriber<? super T> actual, int n, int prefetch, SchedulerService.Worker worker) {
            super(actual, n, prefetch, worker);
        }

        @Override
        public void onNext(JoinInnerSubscriber<T> inner, T value) {
            PlainQueue<T> q = inner.getQueue();

            if (!q.offer(value)) {
                cancelAll();
                onError(new IllegalStateException("Queue full?!"));
                return;
            }
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (errors.compareAndSet(null, e)) {
                cancelAll();
                drain();
            } else {
                if (e != errors.get()) {
                    FolyamPlugins.onError(e);
                }
            }
        }

        @Override
        public void onComplete() {
            done.decrementAndGet();
            drain();
        }

        @Override
        public void run() {
            int missed = 1;

            JoinInnerSubscriber<T>[] s = this.subscribers;
            int n = s.length;
            FolyamSubscriber<? super T> a = this.actual;

            for (;;) {

                long r = requested.get();
                long e = 0;

                middle:
                while (e != r) {
                    if (cancelled) {
                        cleanup();
                        return;
                    }

                    Throwable ex = errors.get();
                    if (ex != null) {
                        cleanup();
                        a.onError(ex);
                        return;
                    }

                    boolean d = done.get() == 0;

                    boolean empty = true;

                    for (int i = 0; i < s.length; i++) {
                        JoinInnerSubscriber<T> inner = s[i];
                        PlainQueue<T> q = inner.queue;
                        if (q != null) {
                            T v = q.poll();

                            if (v != null) {
                                empty = false;
                                a.onNext(v);
                                inner.requestOne();
                                if (++e == r) {
                                    break middle;
                                }
                            }
                        }
                    }

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        cleanup();
                        return;
                    }

                    Throwable ex = errors.get();
                    if (ex != null) {
                        cleanup();
                        a.onError(ex);
                        return;
                    }

                    boolean d = done.get() == 0;

                    boolean empty = true;

                    for (int i = 0; i < n; i++) {
                        JoinInnerSubscriber<T> inner = s[i];

                        FusedQueue<T> q = inner.queue;
                        if (q != null && !q.isEmpty()) {
                            empty = false;
                            break;
                        }
                    }

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }
                }

                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                int w = get();
                if (w == missed) {
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }

    static final class JoinSubscriptionDelayError<T> extends JoinSubscriptionBase<T> {

        private static final long serialVersionUID = -5737965195918321883L;

        JoinSubscriptionDelayError(FolyamSubscriber<? super T> actual, int n, int prefetch, SchedulerService.Worker worker) {
            super(actual, n, prefetch, worker);
        }

        @Override
        void onNext(JoinInnerSubscriber<T> inner, T value) {
            PlainQueue<T> q = inner.getQueue();

            if (!q.offer(value)) {
                if (inner.cancel()) {
                    ExceptionHelper.addThrowable(errors, new IllegalStateException("Queue full?!"));
                    done.decrementAndGet();
                }
            }

            drain();
        }

        @Override
        void onError(Throwable e) {
            ExceptionHelper.addThrowable(errors, e);
            done.decrementAndGet();
            drain();
        }

        @Override
        void onComplete() {
            done.decrementAndGet();
            drain();
        }

        @Override
        public void run() {
            int missed = 1;

            JoinInnerSubscriber<T>[] s = this.subscribers;
            int n = s.length;
            FolyamSubscriber<? super T> a = this.actual;

            for (;;) {

                long r = requested.get();
                long e = 0;

                middle:
                while (e != r) {
                    if (cancelled) {
                        cleanup();
                        return;
                    }

                    boolean d = done.get() == 0;

                    boolean empty = true;

                    for (int i = 0; i < n; i++) {
                        JoinInnerSubscriber<T> inner = s[i];

                        PlainQueue<T> q = inner.queue;
                        if (q != null) {
                            T v = q.poll();

                            if (v != null) {
                                empty = false;
                                a.onNext(v);
                                inner.requestOne();
                                if (++e == r) {
                                    break middle;
                                }
                            }
                        }
                    }

                    if (d && empty) {
                        Throwable ex = errors.get();
                        if (ex != null) {
                            a.onError(ExceptionHelper.terminate(errors));
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        cleanup();
                        return;
                    }

                    boolean d = done.get() == 0;

                    boolean empty = true;

                    for (int i = 0; i < n; i++) {
                        JoinInnerSubscriber<T> inner = s[i];

                        FusedQueue<T> q = inner.queue;
                        if (q != null && !q.isEmpty()) {
                            empty = false;
                            break;
                        }
                    }

                    if (d && empty) {
                        Throwable ex = errors.get();
                        if (ex != null) {
                            a.onError(ExceptionHelper.terminate(errors));
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                }

                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                int w = get();
                if (w == missed) {
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }

    static final class JoinInnerSubscriber<T>
    extends AtomicReference<Flow.Subscription>
    implements FolyamSubscriber<T> {

        private static final long serialVersionUID = 8410034718427740355L;

        final JoinSubscriptionBase<T> parent;

        final int prefetch;

        final int limit;

        long produced;

        volatile PlainQueue<T> queue;

        JoinInnerSubscriber(JoinSubscriptionBase<T> parent, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch ;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            if (SubscriptionHelper.replace(this, s)) {
                s.request(prefetch);
            }
        }

        @Override
        public void onNext(T t) {
            parent.onNext(this, t);
        }

        @Override
        public void onError(Throwable t) {
            parent.onError(t);
        }

        @Override
        public void onComplete() {
            parent.onComplete();
        }

        public void requestOne() {
            long p = produced + 1;
            if (p == limit) {
                produced = 0;
                get().request(p);
            } else {
                produced = p;
            }
        }

        public void request(long n) {
            long p = produced + n;
            if (p >= limit) {
                produced = 0;
                get().request(p);
            } else {
                produced = p;
            }
        }

        public boolean cancel() {
            return SubscriptionHelper.cancel(this);
        }

        PlainQueue<T> getQueue() {
            PlainQueue<T> q = queue;
            if (q == null) {
                q = new SpscArrayQueue<>(prefetch);
                this.queue = q;
            }
            return q;
        }
    }
}
