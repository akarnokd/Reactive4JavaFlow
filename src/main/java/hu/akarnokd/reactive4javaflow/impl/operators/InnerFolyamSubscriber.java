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

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class InnerFolyamSubscriber<T> extends AtomicReference<Flow.Subscription>
implements FolyamSubscriber<T>, AutoDisposable {

    final InnerFolyamSubscriberSupport<T> parent;

    public final int prefetch;

    public FusedQueue<T> queue;
    static final VarHandle QUEUE = VH.find(MethodHandles.lookup(), InnerFolyamSubscriber.class, "queue", FusedQueue.class);

    boolean done;
    static final VarHandle DONE = VH.find(MethodHandles.lookup(), InnerFolyamSubscriber.class, "done", Boolean.TYPE);

    boolean allowRequest;
    int consumed;

    public InnerFolyamSubscriber(InnerFolyamSubscriberSupport<T> parent, int prefetch) {
        this.parent = parent;
        this.prefetch = prefetch;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, subscription)) {
            if (subscription instanceof FusedSubscription) {
                FusedSubscription fs = (FusedSubscription) subscription;

                int m = fs.requestFusion(FusedSubscription.ANY | FusedSubscription.BOUNDARY);

                if (m == FusedSubscription.SYNC) {
                    QUEUE.setRelease(this, fs);
                    DONE.setRelease(this, true);
                    parent.drain();
                    return;
                }
                if (m == FusedSubscription.ASYNC) {
                    QUEUE.setRelease(this, fs);
                }
            }

            allowRequest = true;
            subscription.request(prefetch);
        }
    }

    @Override
    public void onNext(T item) {
        if (item != null) {
            parent.innerNext(this, item);
        } else {
            parent.drain();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        parent.innerError(this, throwable);
    }

    @Override
    public void onComplete() {
        parent.innerComplete(this);
    }

    public void close() {
        SubscriptionHelper.cancel(this);
    }

    public void produced(int n, int limit) {
        if (allowRequest) {
            int c = consumed + n;
            if (c >= limit) {
                consumed = 0;
                getPlain().request(limit);
            } else {
                consumed = c;
            }
        }
    }

    public FusedQueue<T> getOrCreateQueue() {
        FusedQueue<T> q = (FusedQueue<T>)QUEUE.get(this);
        if (q == null) {
            q = createQueue();
        }
        return q;
    }

    public FusedQueue<T> createQueue() {
        FusedQueue<T> q;
        int p = prefetch;
        if (p == 1) {
            q = new SpscOneQueue<>();
        } else {
            q = new SpscArrayQueue<>(p);
        }
        QUEUE.setRelease(this, q);
        return q;
    }

    public FusedQueue<T> getQueue() {
        return (FusedQueue<T>)QUEUE.getAcquire(this);
    }

    public FusedQueue<T> getQueuePlain() {
        return (FusedQueue<T>)QUEUE.get(this);
    }

    public boolean isDone() {
        return (boolean)DONE.getAcquire(this);
    }

    public void setDone() {
        DONE.setRelease(this, true);
    }
}
