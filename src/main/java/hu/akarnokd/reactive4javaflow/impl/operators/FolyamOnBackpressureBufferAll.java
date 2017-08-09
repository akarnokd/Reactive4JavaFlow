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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamOnBackpressureBufferAll<T> extends Folyam<T> {

    final Folyam<T> source;

    final int capacityHint;

    final boolean bounded;

    public FolyamOnBackpressureBufferAll(Folyam<T> source, int capacityHint, boolean bounded) {
        this.source = source;
        this.capacityHint = capacityHint;
        this.bounded = bounded;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnBackpressureBufferConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, capacityHint, bounded));
        } else {
            source.subscribe(new OnBackpressureBufferSubscriber<>(s, capacityHint, bounded));
        }
    }

    static abstract class AbstractOnBackpressureBuffer<T> extends AtomicInteger implements FolyamSubscriber<T>, FusedSubscription<T> {

        final PlainQueue<T> queue;

        Flow.Subscription upstream;

        boolean outputFused;

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE;
        Throwable error;

        long requested;
        static final VarHandle REQUESTED;

        long emitted;

        static {
            try {
                DONE = MethodHandles.lookup().findVarHandle(AbstractOnBackpressureBuffer.class, "done", boolean.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractOnBackpressureBuffer.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractOnBackpressureBuffer(int capacityHint, boolean bounded) {
            queue = bounded ? new SpscArrayQueue<>(capacityHint) : new SpscLinkedArrayQueue<>(capacityHint);
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            subscription.request(Long.MAX_VALUE);
        }

        abstract void onStart();

        @Override
        public final void onNext(T item) {
            if (!queue.offer(item)) {
                upstream.cancel();
                onError(new IllegalStateException("The consumer is not ready to receive items"));
                return;
            }
            drain();
        }

        @Override
        public final void onError(Throwable throwable) {
            error = throwable;
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
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

        abstract void drainLoop();

        abstract void drainFused();

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
        public final T poll() throws Throwable {
            return queue.poll();
        }

        @Override
        public final boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public final void clear() {
            queue.clear();
        }
    }

    static final class OnBackpressureBufferSubscriber<T> extends AbstractOnBackpressureBuffer<T> {

        final FolyamSubscriber<? super T> actual;

        OnBackpressureBufferSubscriber(FolyamSubscriber<? super T> actual, int capacityHint, boolean bounded) {
            super(capacityHint, bounded);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            PlainQueue<T> q = queue;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
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
            PlainQueue<T> q = queue;

            for (;;) {
                if (cancelled) {
                    q.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
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
    }

    static final class OnBackpressureBufferConditionalSubscriber<T> extends AbstractOnBackpressureBuffer<T> {

        final ConditionalSubscriber<? super T> actual;

        OnBackpressureBufferConditionalSubscriber(ConditionalSubscriber<? super T> actual, int capacityHint, boolean bounded) {
            super(capacityHint, bounded);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super T> a = actual;
            PlainQueue<T> q = queue;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
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
            PlainQueue<T> q = queue;
            for (;;) {

                if (cancelled) {
                    q.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                if (!q.isEmpty()) {
                    a.tryOnNext(null);
                }

                if (d) {
                    Throwable ex = error;
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
    }
}
