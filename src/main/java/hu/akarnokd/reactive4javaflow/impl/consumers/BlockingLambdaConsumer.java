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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.locks.*;

public final class BlockingLambdaConsumer<T> extends ReentrantLock implements FolyamSubscriber<T>, AutoDisposable {

    final CheckedConsumer<? super T> onNext;

    final CheckedConsumer<? super Throwable> onError;

    final CheckedRunnable onComplete;

    final int prefetch;

    final Condition condition;

    FusedQueue<T> queue;
    static final VarHandle QUEUE;

    boolean done;
    static final VarHandle DONE;

    Throwable error;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    long wip;
    static final VarHandle WIP;

    int sourceFused;

    static {
        try {
            UPSTREAM = MethodHandles.lookup().findVarHandle(BlockingLambdaConsumer.class, "upstream", Flow.Subscription.class);
            WIP = MethodHandles.lookup().findVarHandle(BlockingLambdaConsumer.class, "wip", Long.TYPE);
            DONE = MethodHandles.lookup().findVarHandle(BlockingLambdaConsumer.class, "done", Boolean.TYPE);
            QUEUE = MethodHandles.lookup().findVarHandle(BlockingLambdaConsumer.class, "queue", FusedQueue.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public BlockingLambdaConsumer(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError, CheckedRunnable onComplete, int prefetch) {
        this.onNext = onNext;
        this.onError = onError;
        this.onComplete = onComplete;
        this.prefetch = prefetch;
        this.condition = newCondition();
        this.queue = new SpscArrayQueue<>(prefetch);
    }

    @Override
    public void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            if (subscription instanceof FusedSubscription) {
                FusedSubscription<T> fs = (FusedSubscription<T>) subscription;
                int m = fs.requestFusion(FusedSubscription.ANY | FusedSubscription.BOUNDARY);
                if (m == FusedSubscription.SYNC) {
                    sourceFused = m;
                    QUEUE.setRelease(this, fs);
                    DONE.setRelease(this, true);
                    signal();
                    return;
                }
                if (m == FusedSubscription.ASYNC) {
                    sourceFused = m;
                    QUEUE.setRelease(this, fs);
                    subscription.request(prefetch);
                    return;
                }
            }

            int p = prefetch;
            QUEUE.setRelease(this, new SpscArrayQueue<>(p));
            subscription.request(p);
        }
    }

    void signal() {
        if ((long)WIP.getAndAdd(this, 1) == 0) {
            lock();
            try {
                condition.signal();
            } finally {
                unlock();
            }
        }
    }

    @Override
    public void onNext(T item) {
        queue.offer(item);
        signal();
    }

    @Override
    public void onError(Throwable throwable) {
        this.error = throwable;
        DONE.setRelease(this, true);
        signal();
    }

    @Override
    public void onComplete() {
        DONE.setRelease(this, true);
        signal();
    }

    void error(Throwable ex) {
        try {
            onError.accept(ex);
        } catch (Throwable exc) {
            FolyamPlugins.onError(exc);
        }
    }

    public void run() {
        FusedQueue<T> q = queue;
        CheckedConsumer<? super T> next = onNext;
        int c = 0;
        int lim = prefetch - (prefetch >> 2);
        long missed = 1;
        for (;;) {

            for (;;) {
                boolean d = (boolean)DONE.getAcquire(this);
                T v;

                try {
                    v = q.poll();
                } catch (Throwable ex) {
                    close();
                    q.clear();
                    error(ex);
                    return;
                }

                boolean empty = v == null;

                if (d && empty) {
                    Throwable ex = error;
                    if (ex == null) {
                        try {
                            onComplete.run();
                        } catch (Throwable exc) {
                            FolyamPlugins.onError(exc);
                        }
                    } else {
                        error(ex);
                    }
                    return;
                }

                if (empty) {
                    break;
                }

                try {
                    next.accept(v);
                } catch (Throwable ex) {
                    close();
                    q.clear();
                    error(ex);
                    return;
                }

                if (++c == lim) {
                    if (sourceFused != FusedSubscription.SYNC) {
                        c = 0;
                        upstream.request(lim);
                    }
                }
            }

            missed = (long)WIP.getAndAdd(this, -missed) - missed;
            if (missed == 0L) {
                lock();
                try {
                    while ((long)WIP.getAcquire(this) == 0L && !(boolean)DONE.getAcquire(this)) {
                        condition.await();
                    }
                } catch (InterruptedException ex) {
                    close();
                    error(ex);
                    return;
                } finally {
                    unlock();
                }
            }
        }
    }
}
