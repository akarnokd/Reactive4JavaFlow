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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.concurrent.Flow;

public final class FolyamTake<T> extends Folyam<T> {

    final Folyam<T> source;

    final long n;

    public FolyamTake(Folyam<T> source, long n) {
        this.source = source;
        this.n = n;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TakeConditionalSubscriber<>((ConditionalSubscriber<? super T>) s, n));
        } else {
            source.subscribe(new TakeSubscriber<>(s, n));
        }
    }

    static abstract class AbstractTakeSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        long remaining;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        boolean done;

        int fusionMode;

        AbstractTakeSubscriber(long remaining) {
            this.remaining = remaining;
        }

        abstract void onStart();

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            onStart();
        }

        @Override
        public final T poll() throws Throwable {
            long r = remaining;
            if (r < 0L) {
                return null;
            }
            if (r == 0L) {
                if (fusionMode == ASYNC) {
                    onComplete();
                }
                r = -1L;
                return null;
            }
            T v = qs.poll();
            if (v == null) {
                return null;
            }
            remaining = --r;
            if (r == 0L && fusionMode == ASYNC) {
                cancel();
            }
            return v;
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty() || remaining == 0L;
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @Override
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null) {
                int m = fs.requestFusion(mode);
                fusionMode = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            upstream.cancel();
        }
    }

    static final class TakeSubscriber<T> extends AbstractTakeSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        TakeSubscriber(FolyamSubscriber<? super T> actual, long remaining) {
            super(remaining);
            this.actual = actual;
        }

        @Override
        void onStart() {
            if (remaining == 0) {
                upstream.cancel();
                done = true;
                EmptySubscription.complete(actual);
            } else {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T item) {
            if (done) {
                return;
            }
            if (item == null) {
                actual.onNext(null);
                return;
            }
            long r = remaining - 1;

            actual.onNext(item);

            if (r == 0L) {
                upstream.cancel();
                onComplete();
            }
            remaining = r;
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }

    static final class TakeConditionalSubscriber<T> extends AbstractTakeSubscriber<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        TakeConditionalSubscriber(ConditionalSubscriber<? super T> actual, long remaining) {
            super(remaining);
            this.actual = actual;
        }

        @Override
        void onStart() {
            if (remaining == 0) {
                upstream.cancel();
                done = true;
                EmptySubscription.complete(actual);
            } else {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item) && !done) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if (done) {
                return false;
            }
            if (item == null) {
                return actual.tryOnNext(null);
            }
            long r = remaining - 1;

            boolean b = actual.tryOnNext(item);

            remaining = r;
            if (r == 0L) {
                upstream.cancel();
                onComplete();
            }
            return b;
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }
}
