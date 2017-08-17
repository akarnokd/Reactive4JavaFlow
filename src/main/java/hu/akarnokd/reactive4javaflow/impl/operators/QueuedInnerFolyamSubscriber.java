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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class QueuedInnerFolyamSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<T> {

    final QueuedFolyamSubscriberSupport<T> parent;

    final int index;

    final int prefetch;

    final int limit;

    boolean done;
    static final VarHandle DONE = VH.find(MethodHandles.lookup(), QueuedInnerFolyamSubscriber.class, "done", Boolean.TYPE);

    FusedQueue<T> queue;
    static final VarHandle QUEUE = VH.find(MethodHandles.lookup(), QueuedInnerFolyamSubscriber.class, "queue", FusedQueue.class);

    int consumed;

    boolean allowRequest;

    public QueuedInnerFolyamSubscriber(QueuedFolyamSubscriberSupport<T> parent, int index, int prefetch) {
        this.parent = parent;
        this.index = index;
        this.prefetch = prefetch;
        this.limit = prefetch - (prefetch >> 2);
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
                    allowRequest = true;
                    QUEUE.setRelease(this, fs);
                    subscription.request(prefetch);
                    return;
                }
            }

            allowRequest = true;
            int pf = prefetch;
            if (pf == 1) {
                QUEUE.setRelease(this, new SpscOneQueue<>());
            } else {
                QUEUE.setRelease(this, new SpscArrayQueue<>(pf));
            }
            subscription.request(pf);
        }
    }

    @Override
    public void onNext(T item) {
        if (item != null) {
            queue.offer(item);
        }
        parent.drain();
    }

    @Override
    public void onError(Throwable throwable) {
        setPlain(SubscriptionHelper.CANCELLED);
        parent.innerError(this, index, throwable);
    }

    @Override
    public void onComplete() {
        setPlain(SubscriptionHelper.CANCELLED);
        DONE.setRelease(this, true);
        parent.drain();
    }

    public void request() {
        if (allowRequest) {
            int c = consumed + 1;
            if (c == limit) {
                consumed = 0;
                getPlain().request(c);
            } else {
                consumed = c;
            }
        }
    }

    public void cancel() {
        SubscriptionHelper.cancel(this);
    }

    public void clear() {
        FusedQueue<T> q = getQueue();
        if (q != null) {
            q.clear();
        }
    }

    public FusedQueue<T> getQueue() {
        return (FusedQueue<T>)QUEUE.getAcquire(this);
    }

    public boolean isDone() {
        return (boolean)DONE.getAcquire(this);
    }

    public void setDone() {
        DONE.setRelease(this, true);
    }
}