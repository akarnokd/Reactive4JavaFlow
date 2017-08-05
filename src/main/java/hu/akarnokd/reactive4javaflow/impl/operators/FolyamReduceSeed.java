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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiFunction;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.Objects;
import java.util.concurrent.*;

public final class FolyamReduceSeed<T, R> extends Esetleg<R> {

    final Folyam<T> source;

    final Callable<? extends R> initialSupplier;

    final CheckedBiFunction<R, ? super T, R> reducer;

    public FolyamReduceSeed(Folyam<T> source, Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> reducer) {
        this.source = source;
        this.initialSupplier = initialSupplier;
        this.reducer = reducer;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        R seed;

        try {
            seed = Objects.requireNonNull(initialSupplier.call(), "The initialSupplier returned a null value");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        source.subscribe(new ReduceSeedSubscriber<>(s, seed, reducer));
    }

    static final class ReduceSeedSubscriber<T, R> extends DeferredScalarSubscription<R> implements FolyamSubscriber<T> {

        final CheckedBiFunction<R, ? super T, R> reducer;

        Flow.Subscription upstream;

        ReduceSeedSubscriber(FolyamSubscriber<? super R> actual, R seed, CheckedBiFunction<R, ? super T, R> reducer) {
            super(actual);
            value = seed;
            this.reducer = reducer;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            if (getPlain() != CANCELLED) {
                try {
                    value = Objects.requireNonNull(reducer.apply(value, item), "The reducer returned a null value");
                } catch (Throwable ex) {
                    value = null;
                    upstream.cancel();
                    error(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            value = null;
            error(throwable);
        }

        @Override
        public void onComplete() {
            complete(value);
        }

        @Override
        public void cancel() {
            super.cancel();
            upstream.cancel();
        }
    }
}
