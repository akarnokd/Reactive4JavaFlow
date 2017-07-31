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

import java.util.concurrent.Flow;

public final class FolyamSkip<T> extends Folyam<T> {

    final Folyam<T> source;

    final long n;


    public FolyamSkip(Folyam<T> source, long n) {
        this.source = source;
        this.n = n;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new SkipConditionalSubscriber<>((ConditionalSubscriber<? super T>) s, n));
        } else {
            source.subscribe(new SkipSubscriber<>(s, n));
        }
    }

    static abstract class AbstractSkipSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        long remaining;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        int fusionMode;

        AbstractSkipSubscriber(long remaining) {
            this.remaining = remaining;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }

            long n = remaining;

            onStart();

            if (fusionMode != SYNC) {
                subscription.request(n);
            }
        }

        @Override
        public final T poll() throws Throwable {
            long r = remaining;
            if (r != 0L) {
                do {
                    T v = qs.poll();
                    if (v == null) {
                        remaining = r;
                        return null;
                    }
                    r--;
                } while (r != 0L);
                remaining = 0;
            }
            return qs.poll();
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @Override
        public final void cancel() {
            upstream.cancel();
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final int requestFusion(int mode) {
            if (qs != null) {
                int m = qs.requestFusion(mode);
                fusionMode = m;
                return m;
            }
            return NONE;
        }

        abstract void onStart();
    }

    static final class SkipSubscriber<T> extends AbstractSkipSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        SkipSubscriber(FolyamSubscriber<? super T> actual, long remaining) {
            super(remaining);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                actual.onNext(null);
            } else {
                long r = remaining;

                if (r == 0) {
                    actual.onNext(item);
                } else {
                    remaining = r - 1;
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }

    static final class SkipConditionalSubscriber<T> extends AbstractSkipSubscriber<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        SkipConditionalSubscriber(ConditionalSubscriber<? super T> actual, long remaining) {
            super(remaining);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if (item == null) {
                return actual.tryOnNext(null);
            }
            long r = remaining;

            if (r == 0) {
                return actual.tryOnNext(item);
            }
            remaining = r - 1;
            return true;
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
}
