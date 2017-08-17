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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class MulticastProcessor<T> extends FolyamProcessor<T> implements AutoDisposable {

    final int prefetch;

    MulticastSubscription<T>[] subscribers;
    static final VarHandle SUBSCRIBERS = VH.find(MethodHandles.lookup(), MulticastProcessor.class, "subscribers", MulticastSubscription[].class);

    static final MulticastSubscription[] EMPTY = new MulticastSubscription[0];
    static final MulticastSubscription[] TERMINATED = new MulticastSubscription[0];

    int wip;
    static final VarHandle WIP = VH.find(MethodHandles.lookup(), MulticastProcessor.class, "wip", int.class);

    Throwable error;
    static final VarHandle ERROR = VH.find(MethodHandles.lookup(), MulticastProcessor.class, "error", Throwable.class);

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), MulticastProcessor.class, "upstream", Flow.Subscription.class);

    FusedQueue<T> queue;
    static final VarHandle QUEUE = VH.find(MethodHandles.lookup(), MulticastProcessor.class, "queue", FusedQueue.class);

    int consumed;

    int sourceFused;

    public MulticastProcessor() {
        this(FolyamPlugins.defaultBufferSize());
    }

    public MulticastProcessor(int prefetch) {
        this.prefetch = prefetch;
        SUBSCRIBERS.setRelease(this, EMPTY);
    }

    @Override
    public boolean hasThrowable() {
        Throwable ex = (Throwable)ERROR.getAcquire(this);
        return ex != null && ex != ExceptionHelper.TERMINATED;
    }

    @Override
    public Throwable getThrowable() {
        Throwable ex = (Throwable)ERROR.getAcquire(this);
        return ex != ExceptionHelper.TERMINATED ? ex : null;
    }

    @Override
    public boolean hasComplete() {
        return ERROR.getAcquire(this) == ExceptionHelper.TERMINATED;
    }

    @Override
    public boolean hasSubscribers() {
        return ((MulticastSubscription[])SUBSCRIBERS.getAcquire(this)).length != 0;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        MulticastSubscription<T> ms = new MulticastSubscription<>(s, this);
        s.onSubscribe(ms);
        if (add(ms)) {
            if (ms.isCancelled()) {
                remove(ms);
            } else {
                drain();
            }
        } else {
            Throwable ex = (Throwable)ERROR.getAcquire(this);
            if (ex == ExceptionHelper.TERMINATED) {
                ms.actual.onComplete();
            } else {
                ms.actual.onError(ex);
            }
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            if (subscription instanceof FusedSubscription) {
                FusedSubscription<T> fs = (FusedSubscription<T>) subscription;
                int m = fs.requestFusion(FusedSubscription.ANY);

                if (m == FusedSubscription.SYNC) {
                    sourceFused = m;
                    QUEUE.setRelease(this, fs);
                    ERROR.setRelease(this, ExceptionHelper.TERMINATED);
                    drain();
                    return;
                }
                if (m == FusedSubscription.ASYNC) {
                    sourceFused = m;
                    QUEUE.setRelease(this, fs);
                    subscription.request(prefetch);
                    return;
                }
            }

            int pf = prefetch;
            if (pf == 1) {
                QUEUE.setRelease(this, new SpscOneQueue<>());
            } else {
                QUEUE.setRelease(this, new SpscArrayQueue<>(pf));
            }

            subscription.request(pf);
        }
    }

    public void start() {
        onSubscribe(new BooleanSubscription());
    }

    public boolean prepare(Flow.Subscription subscription) {
        return UPSTREAM.compareAndSet(this, null, subscription);
    }

    public boolean hasTerminated() {
        return ERROR.getAcquire(this) != null;
    }

    @Override
    public void onNext(T item) {
        if (error != null) {
            return;
        }
        if (item != null) {
            if (!queue.offer(item)) {
                onError(new IllegalStateException("Not all consumers are ready to receive items"));
            }
        }
        drain();
    }

    public boolean tryOnNext(T item) {
        if (error != null) {
            return true;
        }
        if (sourceFused == FusedSubscription.NONE) {
            if (queue.offer(item)) {
                drain();
                return true;
            }
            return false;
        }
        throw new IllegalStateException("MulticastProcessor already consuming items from an upstream source");
    }

    @Override
    public void onError(Throwable throwable) {
        if (error == null && ERROR.compareAndSet(this, null, throwable)) {
            drain();
        } else {
            FolyamPlugins.onError(throwable);
        }
    }

    @Override
    public void onComplete() {
        if (ERROR.compareAndSet(this, null, ExceptionHelper.TERMINATED)) {
            drain();
        }
    }

    @Override
    public void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
        if (ERROR.getAcquire(this) == null && ERROR.compareAndSet(this, null, new CancellationException("MulticastProcessor closed"))) {
            drain();
        }
    }

    @SuppressWarnings("unchecked")
    boolean add(MulticastSubscription<T> ds) {
        for (;;) {
            MulticastSubscription<T>[] a = (MulticastSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            MulticastSubscription<T>[] b = new MulticastSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ds;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(MulticastSubscription<T> ds) {
        for (;;) {
            MulticastSubscription<T>[] a = (MulticastSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            int n = a.length;
            if (n == 0) {
                return;
            }
            int j = -1;
            for (int i = 0; i < n; i++) {
                if (ds == a[i]) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                break;
            }
            MulticastSubscription<T>[] b;
            if (n == 1) {
                b = EMPTY;
            } else {
                b = new MulticastSubscription[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                break;
            }
        }
    }

    void drain() {
        if ((int)WIP.getAndAdd(this, 1) == 0) {
            for (;;) {
                int missed = 1;
                FusedQueue<T> q = (FusedQueue<T>) QUEUE.getAcquire(this);
                if (q != null) {
                    if (sourceFused == FusedSubscription.SYNC) {
                        drainSync(q);
                    } else {
                        drainNormal(q);
                    }
                    return;
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    void drainSync(FusedQueue<T> q) {
        int missed = 1;
        for (;;) {

            for (;;) {
                MulticastSubscription<T>[] subs = (MulticastSubscription<T>[]) SUBSCRIBERS.getAcquire(this);

                int n = subs.length;
                int ready = 0;
                int active = 0;

                for (MulticastSubscription<T> ms : subs) {
                    long r = ms.getAcquire();
                    if (r != Long.MIN_VALUE) {
                        active++;
                        if (r - ms.emitted != 0) {
                            ready++;
                        }
                    }
                }

                if (active == 0 || active != ready) {
                    if (q.isEmpty()) {
                        terminate((Throwable)ERROR.getAcquire(this));
                        return;
                    }
                    break;
                }

                T v;

                try {
                    v = q.poll();
                } catch (Throwable ex) {
                    upstream.cancel();
                    if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                        ex = (Throwable)ERROR.getAcquire(this);
                    } else {
                        ERROR.setRelease(this, ex);
                    }
                    terminate(ex);
                    return;
                }

                boolean empty = v == null;

                if (empty) {
                    terminate((Throwable)ERROR.getAcquire(this));
                    return;
                }

                for (MulticastSubscription<T> ms : subs) {
                    ms.onNext(v);
                }
            }

            missed = (int)WIP.getAndAdd(this, -missed) - missed;
            if (missed == 0) {
                break;
            }
        }
    }

    void terminate(Throwable ex) {
        if (ex == ExceptionHelper.TERMINATED) {
            for (MulticastSubscription<?> ms : (MulticastSubscription<?>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                ms.onComplete();
            }
        } else {
            for (MulticastSubscription<?> ms : (MulticastSubscription<?>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                ms.onError(ex);
            }
        }
    }

    void drainNormal(FusedQueue<T> q) {
        int missed = 1;
        int pf = prefetch;
        int limit = pf - (pf >> 2);
        int c = consumed;
        for (;;) {

            for (;;) {
                MulticastSubscription<T>[] subs = (MulticastSubscription<T>[]) SUBSCRIBERS.getAcquire(this);

                int n = subs.length;
                int ready = 0;
                int active = 0;

                for (MulticastSubscription<T> ms : subs) {
                    long r = ms.getAcquire();
                    if (r != Long.MIN_VALUE) {
                        active++;
                        if (r - ms.emitted != 0) {
                            ready++;
                        }
                    }
                }

                boolean d = ERROR.getAcquire(this) != null;

                if (active == 0 || active != ready) {
                    if (d && q.isEmpty()) {
                        terminate((Throwable)ERROR.getAcquire(this));
                        return;
                    }
                    break;
                }

                T v;

                try {
                    v = q.poll();
                } catch (Throwable ex) {
                    upstream.cancel();
                    if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                        ex = (Throwable)ERROR.getAcquire(this);
                    } else {
                        ERROR.setRelease(this, ex);
                    }
                    terminate(ex);
                    return;
                }

                boolean empty = v == null;

                if (d && empty) {
                    terminate((Throwable)ERROR.getAcquire(this));
                    return;
                }

                if (empty) {
                    break;
                }

                for (MulticastSubscription<T> ms : subs) {
                    ms.onNext(v);
                }

                if (++c == limit) {
                    c = 0;
                    upstream.request(limit);
                }
            }

            consumed = c;
            missed = (int)WIP.getAndAdd(this, -missed) - missed;
            if (missed == 0) {
                break;
            }
        }
    }

    static final class MulticastSubscription<T> extends AtomicLong implements Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final MulticastProcessor<T> parent;

        long emitted;

        MulticastSubscription(FolyamSubscriber<? super T> actual, MulticastProcessor<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequestedCancellable(this, n);
            parent.drain();
        }

        @Override
        public void cancel() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                parent.remove(this);
                parent.drain();
            }
        }

        boolean isCancelled() {
            return getAcquire() == Long.MIN_VALUE;
        }

        void onNext(T item) {
            if (getAcquire() != Long.MIN_VALUE) {
                emitted++;
                actual.onNext(item);
            }
        }

        void onError(Throwable ex) {
            if (getAcquire() != Long.MIN_VALUE) {
                setRelease(Long.MIN_VALUE);
                actual.onError(ex);
            }
        }

        void onComplete() {
            if (getAcquire() != Long.MIN_VALUE) {
                setRelease(Long.MIN_VALUE);
                actual.onComplete();
            }
        }
    }
}
