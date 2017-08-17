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

import java.lang.invoke.*;
import java.util.ArrayDeque;
import java.util.concurrent.Flow;

public final class FolyamTakeLast<T> extends Folyam<T> {

    final Folyam<T> source;

    final int n;

    public FolyamTakeLast(Folyam<T> source, int n) {
        this.source = source;
        this.n = n;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TakeLastConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, n));
        } else {
            source.subscribe(new TakeLastSubscriber<>(s, n));
        }
    }

    static abstract class AbstractTakeLast<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        final int n;

        final ArrayDeque<T> queue;

        Flow.Subscription upstream;

        boolean cancelled;
        static final VarHandle CANCELLED = VH.find(MethodHandles.lookup(), AbstractTakeLast.class, "cancelled", Boolean.TYPE);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractTakeLast.class, "requested", Long.TYPE);

        boolean outputFused;

        AbstractTakeLast(int n) {
            this.queue = new ArrayDeque<>();
            this.n = n;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            subscription.request(Long.MAX_VALUE);
        }


        @Override
        public final void onNext(T item) {
            ArrayDeque<T> q = this.queue;
            if (q.size() == n) {
                q.poll();
            }
            q.offer(item);
        }


        @Override
        public final void cancel() {
            CANCELLED.setVolatile(this, true);
            upstream.cancel();
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

        abstract void onStart();
    }

    static final class TakeLastSubscriber<T> extends AbstractTakeLast<T> {

        final FolyamSubscriber<? super T> actual;

        TakeLastSubscriber(FolyamSubscriber<? super T> actual, int n) {
            super(n);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onError(Throwable throwable) {
            queue.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (outputFused) {
                actual.onNext(null);
                actual.onComplete();
            } else {
                SubscriptionHelper.postComplete(actual, queue, this, REQUESTED, CANCELLED);
            }
        }

        @Override
        public void request(long n) {
            if (!outputFused) {
                SubscriptionHelper.postCompleteRequest(actual, queue, this, REQUESTED, CANCELLED, n);
            }
        }
    }

    static final class TakeLastConditionalSubscriber<T> extends AbstractTakeLast<T> {

        final ConditionalSubscriber<? super T> actual;

        TakeLastConditionalSubscriber(ConditionalSubscriber<? super T> actual, int n) {
            super(n);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onError(Throwable throwable) {
            queue.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (outputFused) {
                actual.tryOnNext(null);
                actual.onComplete();
            } else {
                SubscriptionHelper.postComplete(actual, queue, this, REQUESTED, CANCELLED);
            }
        }

        @Override
        public void request(long n) {
            if (!outputFused) {
                SubscriptionHelper.postCompleteRequest(actual, queue, this, REQUESTED, CANCELLED, n);
            }
        }
    }
}
