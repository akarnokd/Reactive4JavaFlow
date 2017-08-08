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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.ArrayDeque;
import java.util.concurrent.Flow;

public final class FolyamSkipLast<T> extends Folyam<T> {

    final Folyam<T> source;

    final int n;

    public FolyamSkipLast(Folyam<T> source, int n) {
        this.source = source;
        this.n = n;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new SkipLastConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, n));
        } else {
            source.subscribe(new SkipLastSubscriber<>(s, n));
        }
    }

    static final class SkipLastSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final int n;

        final ArrayDeque<T> queue;

        Flow.Subscription upstream;

        SkipLastSubscriber(FolyamSubscriber<? super T> actual, int n) {
            this.actual = actual;
            this.n = n;
            this.queue = new ArrayDeque<>();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(n);
        }

        @Override
        public void onNext(T item) {
            ArrayDeque<T> q = this.queue;
            if (q.size() == n) {
                actual.onNext(q.poll());
            }
            q.offer(item);
        }

        @Override
        public void onError(Throwable throwable) {
            queue.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            queue.clear();
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }

    static final class SkipLastConditionalSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final ConditionalSubscriber<? super T> actual;

        final int n;

        final ArrayDeque<T> queue;

        Flow.Subscription upstream;

        SkipLastConditionalSubscriber(ConditionalSubscriber<? super T> actual, int n) {
            this.actual = actual;
            this.n = n;
            this.queue = new ArrayDeque<>();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(n);
        }

        @Override
        public void onNext(T item) {
            ArrayDeque<T> q = this.queue;
            if (q.size() == n) {
                actual.onNext(q.poll());
            }
            q.offer(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            ArrayDeque<T> q = this.queue;
            boolean b = true;
            if (q.size() == n) {
                b = actual.tryOnNext(q.poll());
            }
            q.offer(item);
            return b;
        }

        @Override
        public void onError(Throwable throwable) {
            queue.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            queue.clear();
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }
}
