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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class FolyamBufferBoundary<T, C extends Collection<? super T>> extends Folyam<C> {

    final Folyam<T> source;

    final Flow.Publisher<?> boundary;

    final Callable<C> collectionSupplier;

    final int maxSize;

    public FolyamBufferBoundary(Folyam<T> source, Flow.Publisher<?> boundary, Callable<C> collectionSupplier, int maxSize) {
        this.source = source;
        this.boundary = boundary;
        this.collectionSupplier = collectionSupplier;
        this.maxSize = maxSize;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super C> s) {
        BufferBoundarySubscriber<T, C> parent = new BufferBoundarySubscriber<>(s, collectionSupplier, maxSize);
        s.onSubscribe(parent);
        boundary.subscribe(parent.boundarySubscriber);
        source.subscribe(parent);
    }

    static final class BufferBoundarySubscriber<T, C extends Collection<? super T>> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super C> actual;

        final Callable<C> collectionSupplier;

        final int maxSize;

        final BoundarySubscriber<T> boundarySubscriber;

        final SpscLinkedArrayQueue<T> queue;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        Flow.Subscription boundary;
        static final VarHandle BOUNDARY;

        long requested;
        static final VarHandle REQUESTED;

        long available;
        static final VarHandle AVAILABLE;

        long signal;
        static final VarHandle SIGNAL;

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE;

        Throwable error;
        static final VarHandle ERROR;

        int count;
        C buffer;

        long emitted;

        int consumed;

        static {
            try {
                UPSTREAM = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "upstream", Flow.Subscription.class);
                BOUNDARY = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "boundary", Flow.Subscription.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "requested", long.class);
                AVAILABLE = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "available", long.class);
                SIGNAL = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "signal", long.class);
                DONE = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "done", boolean.class);
                ERROR = MethodHandles.lookup().findVarHandle(BufferBoundarySubscriber.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        BufferBoundarySubscriber(FolyamSubscriber<? super C> actual, Callable<C> collectionSupplier, int maxSize) {
            this.actual = actual;
            this.collectionSupplier = collectionSupplier;
            this.maxSize = maxSize;
            this.boundarySubscriber = new BoundarySubscriber<>(this);
            this.queue = new SpscLinkedArrayQueue<>(FolyamPlugins.defaultBufferSize());
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
                subscription.request(maxSize);
            }
        }

        @Override
        public void onNext(T item) {
            queue.offer(item);
            drain();
        }

        @Override
        public void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                SubscriptionHelper.cancel(this, BOUNDARY);
                DONE.setRelease(this, true);
                drain();
            }
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.cancel(this, BOUNDARY);
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, BOUNDARY, REQUESTED, n);
            SubscriptionHelper.addRequested(this, AVAILABLE, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this, UPSTREAM);
            SubscriptionHelper.cancel(this, BOUNDARY);
            if (getAndIncrement() == 0) {
                buffer = null;
                queue.clear();
            }
        }

        void boundaryOnSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, BOUNDARY, REQUESTED, subscription);
        }

        void boundaryNext() {
            SIGNAL.getAndAdd(this, 1);
            drain();
        }

        void boundaryError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                DONE.setRelease(this, true);
                drain();
            }
        }

        void boundaryComplete() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            DONE.setRelease(this, true);
            drain();
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            SpscLinkedArrayQueue<T> q = queue;
            FolyamSubscriber<? super C> a = actual;
            long e = emitted;
            int m = maxSize;
            int lim = m - (m >> 2);
            int c = consumed;

            for (;;) {

                long r = (long)AVAILABLE.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        buffer = null;
                        q.clear();
                        return;
                    }

                    if (ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        buffer = null;
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = q.isEmpty();
                    C buf = buffer;

                    if (d && empty) {
                        if (buf != null && count != 0) {
                            buffer = null;
                            a.onNext(buf);
                        }
                        a.onComplete();
                        return;
                    }

                    if (buf == null) {
                        try {
                            buf = Objects.requireNonNull(collectionSupplier.call(), "The collectionSupplier returned a null collection");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            SubscriptionHelper.cancel(this, UPSTREAM);
                            SubscriptionHelper.cancel(this, BOUNDARY);
                            ex = ExceptionHelper.terminate(this, ERROR);
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                        buffer = buf;
                    }

                    int cnt = count;

                    for (int i = cnt; i < m; i++) {
                        T v = q.poll();
                        if (v == null) {
                            break;
                        }
                        buf.add(v);
                        cnt++;

                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }
                    }

                    long sig = (long)SIGNAL.getAcquire(this);
                    boolean capacityNotReached = cnt < m;
                    if (capacityNotReached && sig == 0) {
                        count = cnt;
                        break;
                    }

                    a.onNext(buf);

                    buffer = null;
                    count = 0;

                    e++;
                    if (capacityNotReached) {
                        SIGNAL.getAndAdd(this, -1);
                    }
                }

                if (cancelled) {
                    buffer = null;
                    q.clear();
                    return;
                }

                if (ERROR.getAcquire(this) != null) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    buffer = null;
                    q.clear();
                    a.onError(ex);
                    return;
                }

                if ((boolean)DONE.getAcquire(this) && q.isEmpty() && buffer == null) {
                    a.onComplete();
                    return;
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

    static final class BoundarySubscriber<T> implements FolyamSubscriber<Object> {

        final BufferBoundarySubscriber<T, ?> parent;

        BoundarySubscriber(BufferBoundarySubscriber<T, ?> parent) {
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            parent.boundaryOnSubscribe(subscription);
        }

        @Override
        public void onNext(Object item) {
            parent.boundaryNext();
        }

        @Override
        public void onError(Throwable throwable) {
            parent.boundaryError(throwable);
        }

        @Override
        public void onComplete() {
            parent.boundaryComplete();
        }
    }
}
