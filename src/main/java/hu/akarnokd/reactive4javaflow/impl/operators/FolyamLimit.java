/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Limits both the total request amount and items received from the upstream.
 *
 * @param <T> the source and output value type
 * @since 0.1.4
 */
public final class FolyamLimit<T> extends Folyam<T> {

    final Folyam<T> source;

    final long n;

    public FolyamLimit(Folyam<T> source, long n) {
        this.source = source;
        this.n = n;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new LimitSubscriber<>(s, n));
    }

    static final class LimitSubscriber<T>
    extends AtomicLong
    implements FolyamSubscriber<T>, Flow.Subscription {

        private static final long serialVersionUID = 2288246011222124525L;

        final FolyamSubscriber<? super T> actual;

        long remaining;

        Flow.Subscription upstream;

        LimitSubscriber(FolyamSubscriber<? super T> actual, long remaining) {
            this.actual = actual;
            this.remaining = remaining;
            lazySet(remaining);
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            if (remaining == 0L) {
                s.cancel();
                EmptySubscription.complete(actual);
            } else {
                this.upstream = s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            long r = remaining;
            if (r > 0L) {
                remaining = --r;
                actual.onNext(t);
                if (r == 0L) {
                    upstream.cancel();
                    actual.onComplete();
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (remaining > 0L) {
                remaining = 0L;
                actual.onError(t);
            } else {
                FolyamPlugins.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (remaining > 0L) {
                remaining = 0L;
                actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            for (;;) {
                long r = get();
                if (r == 0L) {
                    break;
                }
                long toRequest;
                if (r <= n) {
                    toRequest = r;
                } else {
                    toRequest = n;
                }
                long u = r - toRequest;
                if (compareAndSet(r, u)) {
                    upstream.request(toRequest);
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

    }
}