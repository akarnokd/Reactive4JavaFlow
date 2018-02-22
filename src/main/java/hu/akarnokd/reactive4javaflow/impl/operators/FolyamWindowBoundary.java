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
import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamWindowBoundary<T> extends Folyam<Folyam<T>> {

    final Folyam<T> source;

    final Flow.Publisher<?> boundary;

    final int maxSize;

    public FolyamWindowBoundary(Folyam<T> source, Flow.Publisher<?> boundary, int maxSize) {
        this.source = source;
        this.boundary = boundary;
        this.maxSize = maxSize;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Folyam<T>> s) {
        WindowBoundarySubscriber<T> parent = new WindowBoundarySubscriber<>(s, maxSize);
        s.onSubscribe(parent);
        boundary.subscribe(parent.inner);
        source.subscribe(parent);
    }

    static final class WindowBoundarySubscriber<T> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        final FolyamSubscriber<? super Folyam<T>> actual;

        final int maxSize;

        final SpscLinkedArrayQueue<Object> queue;

        final WindowBoundaryInnerSubscriber<T> inner;

        final int hint;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "requested", long.class);

        long requestedMain;
        static final VarHandle REQUESTED_MAIN = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "requestedMain", long.class);

        boolean once;
        static final VarHandle ONCE = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "once", boolean.class);

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "done", boolean.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "error", Throwable.class);

        int state;
        static final VarHandle STATE = VH.find(MethodHandles.lookup(), WindowBoundarySubscriber.class, "state", int.class);

        static final Object ITEM = new Object();

        static final Object BOUNDARY = new Object();

        SolocastProcessor<T> current;

        int size;

        long emitted;

        WindowBoundarySubscriber(FolyamSubscriber<? super Folyam<T>> actual, int maxSize) {
            this.actual = actual;
            this.maxSize = maxSize;
            int d = FolyamPlugins.defaultBufferSize();
            this.queue = new SpscLinkedArrayQueue<>(d);
            this.hint = Math.min(maxSize, d);
            this.queue.offer(BOUNDARY, BOUNDARY);
            this.inner = new WindowBoundaryInnerSubscriber<>(this);
            STATE.setRelease(this, 1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED_MAIN, subscription);
        }

        @Override
        public void onNext(T item) {
            synchronized (this) {
                queue.offer(ITEM, item);
            }
            drain();
        }

        @Override
        public void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                inner.cancel();
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            inner.cancel();
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            long m = SubscriptionHelper.multiplyCap(maxSize, n);
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED_MAIN, m);
            inner.request(n);
            drain();
        }

        @Override
        public void cancel() {
            inner.cancel();
            if (ONCE.compareAndSet(this, false, true)) {
                run();
            }
        }

        @Override
        public void run() {
            if ((int)STATE.getAndAdd(this, -1) - 1 == 0) {
                SubscriptionHelper.cancel(this, UPSTREAM);
            }
        }

        void innerNext() {
            synchronized (this) {
                queue.offer(BOUNDARY, BOUNDARY);
            }
            drain();
        }

        void innerError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                inner.cancel();
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        void innerComplete() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            DONE.setRelease(this, true);
            drain();
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            long e = emitted;
            SpscLinkedArrayQueue<Object> q = this.queue;
            FolyamSubscriber<? super Folyam<T>> a = this.actual;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if ((int)STATE.getAcquire(this) == 0) {
                        current = null;
                        q.clear();
                        return;
                    }

                    if (ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        q.clear();
                        if (current != null) {
                            current.onError(ex);
                            current = null;
                        }
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = q.isEmpty();

                    if (d & empty) {
                        if (current != null) {
                            current.onComplete();
                            current = null;
                        }
                        a.onComplete();
                        return;
                    }

                    if (e == r || empty) {
                        break;
                    }

                    Object action = q.poll();
                    Object parameter = q.poll();

                    if (action == ITEM) {
                        current.onNext((T)parameter);
                        int s = size + 1;
                        if (s == maxSize) {
                            size = 0;
                            current.onComplete();
                            current = null;
                        } else {
                            size = s;
                            continue;
                        }
                    }
                    STATE.getAndAdd(this, 1);
                    if (current != null) {
                        current.onComplete();
                    }
                    size = 0;
                    current = new SolocastProcessor<>(hint, this);
                    a.onNext(current);
                    e++;
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        static final class WindowBoundaryInnerSubscriber<T> implements FolyamSubscriber<Object> {

            final WindowBoundarySubscriber<T> parent;

            Flow.Subscription upstream;
            static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), WindowBoundaryInnerSubscriber.class, "upstream", Flow.Subscription.class);

            long requested;
            static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), WindowBoundaryInnerSubscriber.class, "requested", long.class);

            WindowBoundaryInnerSubscriber(WindowBoundarySubscriber<T> parent) {
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
            }

            @Override
            public void onNext(Object item) {
                parent.innerNext();
            }

            @Override
            public void onError(Throwable throwable) {
                parent.innerError(throwable);
            }

            @Override
            public void onComplete() {
                parent.innerComplete();
            }

            void request(long n) {
                SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
            }

            void cancel() {
                SubscriptionHelper.cancel(this, UPSTREAM);
            }
        }
    }
}
