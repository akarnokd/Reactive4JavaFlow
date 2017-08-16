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

import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class SolocastProcessor<T> extends FolyamProcessor<T> {

    final SpscLinkedArrayQueue<T> queue;

    long requested;
    static final VarHandle REQUESTED;

    int wip;
    static final VarHandle WIP;

    boolean done;
    static final VarHandle DONE;
    Throwable error;

    FolyamSubscriber<? super T> actual;
    static final VarHandle ACTUAL;

    boolean once;
    static final VarHandle ONCE;

    boolean cancelled;
    static final VarHandle CANCELLED;

    Runnable onTerminate;
    static final VarHandle ON_TERMINATE;

    boolean outputFused;

    long emitted;

    static {
        try {
            REQUESTED = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "requested", Long.TYPE);
            WIP = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "wip", Integer.TYPE);
            DONE = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "done", Boolean.TYPE);
            ACTUAL = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "actual", FolyamSubscriber.class);
            ONCE = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "once", Boolean.TYPE);
            CANCELLED = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "cancelled", Boolean.TYPE);
            ON_TERMINATE = MethodHandles.lookup().findVarHandle(SolocastProcessor.class, "onTerminate", Runnable.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public SolocastProcessor() {
        this(FolyamPlugins.defaultBufferSize());
    }

    public SolocastProcessor(int capacityHint) {
        this.queue = new SpscLinkedArrayQueue<>(capacityHint);
    }

    public SolocastProcessor(int capacityHint, Runnable onTerminate) {
        this(capacityHint);
        ON_TERMINATE.setRelease(this, onTerminate);
    }

    @Override
    public boolean hasThrowable() {
        return (boolean)DONE.getAcquire(this) && error != null;
    }

    @Override
    public Throwable getThrowable() {
        return (boolean)DONE.getAcquire(this) ? error : null;
    }

    @Override
    public boolean hasComplete() {
        return (boolean)DONE.getAcquire(this) && error == null;
    }

    @Override
    public boolean hasSubscribers() {
        return ACTUAL.getAcquire(this) != null;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if ((boolean)ONCE.compareAndSet(this, false, true)) {
            SolocastSubscription<T> parent = new SolocastSubscription<>(this);
            s.onSubscribe(parent);
            if (ACTUAL.compareAndSet(this, null, s)) {
                if ((boolean)CANCELLED.getAcquire(this)) {
                    actual = null;
                } else {
                    drain();
                }
            }
        } else {
            EmptySubscription.error(s, new IllegalStateException("SolocastProcessor allows only one Flow.Subscriber over its lifetime."));
        }
    }

    void terminate() {
        Runnable r = (Runnable)ON_TERMINATE.getAcquire(this);
        if (r != null) {
            r = (Runnable)ON_TERMINATE.getAndSet(this, null);
            if (r != null) {
                r.run();
            }
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (!done && !(boolean)CANCELLED.getAcquire(this)) {
            subscription.request(Long.MAX_VALUE);
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void onNext(T item) {
        Objects.requireNonNull(item, "item == null");
        if (!done && !(boolean)CANCELLED.getAcquire(this)) {
            queue.offer(item);
            drain();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable == null");
        if (!done && !(boolean)CANCELLED.getAcquire(this)) {
            error = throwable;
            DONE.setRelease(this, true);
            drain();
        } else {
            FolyamPlugins.onError(throwable);
        }
        terminate();
    }

    @Override
    public void onComplete() {
        if (!done && !(boolean)CANCELLED.getAcquire(this)) {
            DONE.setRelease(this, true);
            drain();
        }
        terminate();
    }

    @SuppressWarnings("unchecked")
    void drain() {
        if ((int)WIP.getAndAdd(this, 1) == 0) {
            FolyamSubscriber<? super T> a = (FolyamSubscriber<? super T>)ACTUAL.getAcquire(this);
            int missed = 1;
            for (;;) {
                if (a != null) {
                    if (outputFused) {
                        drainFused(a);
                    } else {
                        drainNormal(a);
                    }
                    return;
                }
                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
                a = (FolyamSubscriber<? super T>)ACTUAL.getAcquire(this);
            }
        }
    }

    void drainNormal(FolyamSubscriber<? super T> a) {
        int missed = 1;
        long e = emitted;
        SpscLinkedArrayQueue<T> q = queue;
        for (;;) {
                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        actual = null;
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        actual = null;
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
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
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        q.clear();
                        actual = null;
                        return;
                    }
                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        actual = null;
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                }

            emitted = e;
            missed = (int)WIP.getAndAdd(this, -missed) - missed;
            if (missed == 0) {
                break;
            }
        }
    }

    void drainFused(FolyamSubscriber<? super T> a) {
        int missed = 1;
        SpscLinkedArrayQueue<T> q = queue;
        for (;;) {
                if ((boolean)CANCELLED.getAcquire(this)) {
                    actual = null;
                    q.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                if (!q.isEmpty()) {
                    a.onNext(null);
                }
                if (d) {
                    actual = null;
                    Throwable ex = error;
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

            missed = (int)WIP.getAndAdd(this, -missed) - missed;
            if (missed == 0) {
                break;
            }
        }
    }

    void request(long n) {
        SubscriptionHelper.addRequested(this, REQUESTED, n);
        drain();
    }

    void cancel() {
        CANCELLED.setRelease(this, true);
        actual = null;
        if ((int)WIP.getAndAdd(this, 1) == 0) {
            queue.clear();
        }
        terminate();
    }

    static final class SolocastSubscription<T> implements FusedSubscription<T> {

        final SolocastProcessor<T> parent;

        final SpscLinkedArrayQueue<T> queue;

        SolocastSubscription(SolocastProcessor<T> parent) {
            this.parent = parent;
            this.queue = parent.queue;
        }

        @Override
        public int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                parent.outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public T poll() throws Throwable {
            return queue.poll();
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            queue.clear();
        }

        @Override
        public void request(long n) {
            parent.request(n);
        }

        @Override
        public void cancel() {
            parent.cancel();
        }
    }

}
