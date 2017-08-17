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
import java.util.ArrayDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamWindowSize<T> extends Folyam<Folyam<T>> {

    final Folyam<T> source;

    final int size;

    final int skip;

    public FolyamWindowSize(Folyam<T> source, int size, int skip) {
        this.source = source;
        this.size = size;
        this.skip = skip;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Folyam<T>> s) {
        if (size <= skip) {
            source.subscribe(new WindowSizeNonOverlappingSubscriber<>(s, skip, size));
        } else {
            source.subscribe(new WindowSizeOverlappingSubscriber<>(s, size, skip));
        }
    }

    static final class WindowSizeNonOverlappingSubscriber<T> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        final FolyamSubscriber<? super Folyam<T>> actual;

        final int size;

        final int count;

        final int hint;

        Flow.Subscription upstream;

        int index;

        SolocastProcessor<T> window;

        WindowSizeNonOverlappingSubscriber(FolyamSubscriber<? super Folyam<T>> actual, int size, int count) {
            this.actual = actual;
            this.size = size;
            this.count = count;
            this.hint = Math.min(count, FolyamPlugins.defaultBufferSize());
            this.setRelease(1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            SolocastProcessor<T> w = window;
            int idx = index;
            if (idx == 0) {
                w = new SolocastProcessor<>(hint, this);

                if (!compareAndSet(1, 3)) {
                    return;
                }

                window = w;
            }

            if (w != null) {
                w.onNext(item);
            }
            if (idx == 0) {
                actual.onNext(w);
            }

            idx++;
            if (idx == count) {
                w.onComplete();
                window = null;
            }
            if (idx == size) {
                index = 0;
            } else {
                index = idx;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            SolocastProcessor<T> w = window;
            if (w != null) {
                window = null;
                w.onError(throwable);
            }
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            SolocastProcessor<T> w = window;
            if (w != null) {
                window = null;
                w.onComplete();
            }
            actual.onComplete();
        }

        @Override
        public void run() {
            int s = getAcquire();
            if ((s & 1) == 0 || !compareAndSet(s, s & 1)) {
                upstream.cancel();
            }
        }

        @Override
        public void request(long n) {
            upstream.request(SubscriptionHelper.multiplyCap(size, n));
        }

        @Override
        public void cancel() {
            for (;;) {
                int s = getAcquire();
                if ((s & 1) == 0) {
                    break;
                }
                if (compareAndSet(s, s & 2)) {
                    if ((s & 2) == 0) {
                        upstream.cancel();
                    }
                    break;
                }
            }
        }
    }

    static final class WindowSizeOverlappingSubscriber<T> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        final FolyamSubscriber<? super Folyam<T>> actual;

        final int size;

        final int skip;

        final int hint;

        final SpscLinkedArrayQueue<SolocastProcessor<T>> queue;

        ArrayDeque<SolocastProcessor<T>> windows;

        Flow.Subscription upstream;

        int index;

        int produced;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), WindowSizeOverlappingSubscriber.class, "wip", int.class);

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), WindowSizeOverlappingSubscriber.class, "done", boolean.class);

        Throwable error;

        boolean once;
        static final VarHandle ONCE = VH.find(MethodHandles.lookup(), WindowSizeOverlappingSubscriber.class, "once", boolean.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), WindowSizeOverlappingSubscriber.class, "requested", long.class);

        boolean cancelled;
        static final VarHandle CANCELLED = VH.find(MethodHandles.lookup(), WindowSizeOverlappingSubscriber.class, "cancelled", boolean.class);

        long emitted;

        WindowSizeOverlappingSubscriber(FolyamSubscriber<? super Folyam<T>> actual, int size, int skip) {
            this.actual = actual;
            this.size = size;
            this.skip = skip;
            this.hint = Math.min(size, FolyamPlugins.defaultBufferSize());
            this.windows = new ArrayDeque<>();
            this.queue = new SpscLinkedArrayQueue<>(FolyamPlugins.defaultBufferSize());
            setRelease(1);
        }

        @Override
        public void run() {
            if (decrementAndGet() == 0) {
                upstream.cancel();
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            int idx = index;

            boolean b = false;
            if (idx == 0 && !(boolean)CANCELLED.getAcquire(this)) {
                SolocastProcessor<T> w = new SolocastProcessor<>(hint, this);

                for (;;) {
                    int s = getAcquire();
                    if (s == 0) {
                        return;
                    }
                    if (compareAndSet(s, s + 1)) {
                        break;
                    }
                }

                windows.add(w);
                queue.offer(w);
                b = true;
            }

            for (SolocastProcessor<T> w : windows) {
                w.onNext(item);
            }

            idx++;
            if (idx == skip) {
                index = 0;
            } else {
                index = idx;
            }

            int p = produced + 1;
            if (p == size) {
                produced = p - skip;
                SolocastProcessor<T> w = windows.poll();
                if (w != null) {
                    w.onComplete();
                }
            } else {
                produced = p;
            }

            if (b) {
                drain();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            for (SolocastProcessor<T> b : windows) {
                b.onError(throwable);
            }
            windows.clear();
            error = throwable;
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void onComplete() {
            for (SolocastProcessor<T> b : windows) {
                b.onComplete();
            }
            windows.clear();
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            if (!(boolean)ONCE.getAcquire(this) && ONCE.compareAndSet(this, false, true)) {
                long a = SubscriptionHelper.multiplyCap(n - 1, skip);
                long b = SubscriptionHelper.addCap(a, size);
                upstream.request(b);
            } else {
                long a = SubscriptionHelper.multiplyCap(n, skip);
                upstream.request(a);
            }
            drain();
        }

        @Override
        public void cancel() {
            if (CANCELLED.compareAndSet(this, false, true)) {
                if (decrementAndGet() == 0) {
                    upstream.cancel();
                }
            }
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }
            int missed = 1;
            FolyamSubscriber<? super Folyam<T>> a = actual;
            SpscLinkedArrayQueue<SolocastProcessor<T>> q = this.queue;
            long e = emitted;
            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);

                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                    }

                    SolocastProcessor<T> v = q.poll();

                    boolean empty = v == null;

                    if (d && empty) {
                        actual.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;
                }

                if (e == r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);

                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();
                            a.onError(ex);
                            return;
                        } else if (q.isEmpty()) {
                            a.onComplete();
                            return;
                        }
                    }
                }

                emitted = e;
                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
