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
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamObserveOn<T> extends Folyam<T> {

    final Folyam<T> source;

    final SchedulerService executor;

    final int prefetch;

    public FolyamObserveOn(Folyam<T> source, SchedulerService executor, int prefetch) {
        this.source = source;
        this.executor = executor;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new ObserveOnConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, prefetch, executor.worker()));
        } else {
            source.subscribe(new ObserveOnSubscriber<>(s, prefetch, executor.worker()));
        }
    }

    static abstract class AbstractObserveOn<T> extends AtomicInteger implements FolyamSubscriber<T>, FusedSubscription<T>, Runnable {

        final int prefetch;

        final int limit;

        final SchedulerService.Worker worker;

        Flow.Subscription upstream;

        FusedQueue<T> queue;

        long requested;
        static final VarHandle REQUESTED;

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE;
        Throwable error;

        long emitted;

        int consumed;

        int sourceFused;

        boolean outputFused;

        static {
            try {
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractObserveOn.class, "requested", Long.TYPE);
                DONE = MethodHandles.lookup().findVarHandle(AbstractObserveOn.class, "done", Boolean.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractObserveOn(int prefetch, SchedulerService.Worker worker) {
            this.prefetch = prefetch;
            this.worker = worker;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        @SuppressWarnings("unchecked")
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                FusedSubscription<T> fs = (FusedSubscription<T>) subscription;
                int m = fs.requestFusion(ANY | BOUNDARY);
                if (m == SYNC) {
                    sourceFused = m;
                    queue = fs;
                    DONE.setRelease(this, true);
                    onStart();
                    return;
                }
                if (m == ASYNC) {
                    sourceFused = m;
                    queue = fs;
                    onStart();
                    fs.request(prefetch);
                    return;
                }
            }

            int p = prefetch;
            if (p == 1) {
                queue = new SpscOneQueue<>();
            } else {
                queue = new SpscArrayQueue<>(p);
            }
            onStart();
            subscription.request(p);
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
        public final void onNext(T item) {
            if (item != null) {
                if (!queue.offer(item)) {
                    upstream.cancel();
                    onError(new IllegalStateException("Queue full?! Check the upstream " + upstream.getClass() + " for backpressure bugs!"));
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

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
            worker.close();
            if (getAndIncrement() == 0) {
                queue.clear();
            }
        }

        @Override
        public final T poll() throws Throwable {
            T v = queue.poll();
            if (v != null && sourceFused == ASYNC) {
                int e = consumed + 1;
                if (e == limit) {
                    consumed = 0;
                    upstream.request(e);
                } else {
                    consumed = e;
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
            queue.clear();
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                worker.schedule(this);
            }
        }

        @Override
        public void run() {
            if (outputFused) {
                drainFused();
            } else
            if (sourceFused == SYNC) {
                drainSync();
            } else {
                drainNormal();
            }
        }

        abstract void onStart();

        abstract void drainNormal();

        abstract void drainSync();

        abstract void drainFused();
    }

    static final class ObserveOnSubscriber<T> extends AbstractObserveOn<T> {

        final FolyamSubscriber<? super T> actual;

        ObserveOnSubscriber(FolyamSubscriber<? super T> actual, int prefetch, SchedulerService.Worker worker) {
            super(prefetch, worker);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainNormal() {
            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;
            long e = emitted;
            int c = consumed;
            int lim = limit;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        DONE.setRelease(this, true);
                        q.clear();
                        a.onError(ex);
                        worker.close();
                        return;
                    }
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        worker.close();
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
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    if (d && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        worker.close();
                        return;
                    }
                }

                consumed = c;
                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainSync() {
            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (v == null) {
                        a.onComplete();
                        worker.close();
                        return;
                    }

                    a.onNext(v);
                    e++;
                }

                if (cancelled) {
                    q.clear();
                    return;
                }

                if (q.isEmpty()) {
                    a.onComplete();
                    worker.close();
                    return;
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainFused() {
            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;

            for (;;) {

                if (cancelled) {
                    q.clear();
                    break;
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
                    worker.close();
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

    }

    static final class ObserveOnConditionalSubscriber<T> extends AbstractObserveOn<T> {

        final ConditionalSubscriber<? super T> actual;

        ObserveOnConditionalSubscriber(ConditionalSubscriber<? super T> actual, int prefetch, SchedulerService.Worker worker) {
            super(prefetch, worker);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainNormal() {
            int missed = 1;
            ConditionalSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;
            long e = emitted;
            int c = consumed;
            int lim = limit;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        DONE.setRelease(this, true);
                        q.clear();
                        a.onError(ex);
                        worker.close();
                        return;
                    }
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        worker.close();
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
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    if (d && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        worker.close();
                        return;
                    }
                }

                consumed = c;
                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainSync() {
            int missed = 1;
            ConditionalSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (v == null) {
                        a.onComplete();
                        worker.close();
                        return;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }
                }

                if (cancelled) {
                    q.clear();
                    return;
                }

                if (q.isEmpty()) {
                    a.onComplete();
                    worker.close();
                    return;
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainFused() {
            int missed = 1;
            ConditionalSubscriber<? super T> a = actual;
            FusedQueue<T> q = queue;

            for (;;) {

                if (cancelled) {
                    q.clear();
                    break;
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
                    worker.close();
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

    }
}
