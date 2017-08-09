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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.ArrayDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamOnBackpressureBufferDrop<T> extends Folyam<T> {

    final Folyam<T> source;

    final int capacity;

    final boolean dropNewest;

    final CheckedConsumer<? super T> onDrop;

    public FolyamOnBackpressureBufferDrop(Folyam<T> source, int capacity, boolean dropNewest, CheckedConsumer<? super T> onDrop) {
        this.source = source;
        this.capacity = capacity;
        this.dropNewest = dropNewest;
        this.onDrop = onDrop;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnBackpressureBufferDropConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, capacity, dropNewest, onDrop));
        } else {
            source.subscribe(new OnBackpressureBufferDropSubscriber<>(s, capacity, dropNewest, onDrop));
        }
    }

    static abstract class AbstractOnBackpressureBufferDrop<T> extends AtomicInteger implements FolyamSubscriber<T>, FusedSubscription<T> {

        final int capacity;

        final boolean dropNewest;

        final CheckedConsumer<? super T> onDrop;

        final ArrayDeque<T> queue;

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
                DONE = MethodHandles.lookup().findVarHandle(AbstractOnBackpressureBufferDrop.class, "done", boolean.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractOnBackpressureBufferDrop.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractOnBackpressureBufferDrop(int capacity, boolean dropNewest, CheckedConsumer<? super T> onDrop) {
            this.capacity = capacity;
            this.dropNewest = dropNewest;
            this.onDrop = onDrop;
            this.queue = new ArrayDeque<>();
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
            T dropped = null;
            synchronized (this) {
                ArrayDeque<T> q = this.queue;
                if (q.size() == capacity) {
                    if (dropNewest) {
                        dropped = q.pollLast();
                    } else {
                        dropped = q.pollFirst();
                    }
                }
                q.offer(item);
            }
            if (dropped != null) {
                try {
                    onDrop.accept(dropped);
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
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
                clear();
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
        public final synchronized T poll() {
            return queue.poll();
        }

        @Override
        public final synchronized boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public final synchronized void clear() {
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

        final synchronized void clear(ArrayDeque q) {
            q.clear();
        }

        abstract void drainFused();

        abstract void drainLoop();
    }

    static final class OnBackpressureBufferDropSubscriber<T> extends AbstractOnBackpressureBufferDrop<T> {

        final FolyamSubscriber<? super T> actual;

        protected OnBackpressureBufferDropSubscriber(FolyamSubscriber<? super T> actual, int capacity, boolean dropNewest, CheckedConsumer<? super T> onDrop) {
            super(capacity, dropNewest, onDrop);
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
            long e = emitted;
            ArrayDeque<T> q = queue;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clear(q);
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;
                    synchronized (this) {
                        v = q.poll();
                    }
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
                        clear(q);
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty;
                    synchronized (this) {
                        empty = q.isEmpty();
                    }
                    if (d && empty) {
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

            for (;;) {
                if (cancelled) {
                    clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                a.onNext(null);

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

    static final class OnBackpressureBufferDropConditionalSubscriber<T> extends AbstractOnBackpressureBufferDrop<T> {

        final ConditionalSubscriber<? super T> actual;

        protected OnBackpressureBufferDropConditionalSubscriber(ConditionalSubscriber<? super T> actual, int capacity, boolean dropNewest, CheckedConsumer<? super T> onDrop) {
            super(capacity, dropNewest, onDrop);
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
            long e = emitted;
            ArrayDeque<T> q = queue;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clear(q);
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;
                    synchronized (this) {
                        v = q.poll();
                    }
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
                        clear(q);
                        return;
                    }
                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty;
                    synchronized (this) {
                        empty = q.isEmpty();
                    }
                    if (d && empty) {
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

            for (;;) {
                if (cancelled) {
                    clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                a.tryOnNext(null);

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
