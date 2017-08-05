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
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamReduce<T> extends Esetleg<T> {

    final Folyam<T> source;

    final CheckedBiFunction<T, T, T> reducer;

    public FolyamReduce(Folyam<T> source, CheckedBiFunction<T, T, T> reducer) {
        this.source = source;
        this.reducer = reducer;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new ReduceSubscriber<>(s, reducer));
    }

    static final class ReduceSubscriber<T> extends DeferredScalarSubscription<T> implements FolyamSubscriber<T> {

        final CheckedBiFunction<T, T, T> reducer;

        Flow.Subscription upstream;

        ReduceSubscriber(FolyamSubscriber<? super T> actual, CheckedBiFunction<T, T, T> reducer) {
            super(actual);
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
                T v = value;
                if (v == null) {
                    value = item;
                } else {
                    try {
                        value = Objects.requireNonNull(reducer.apply(v, item), "The reducer returned a null value");
                    } catch (Throwable ex) {
                        value = null;
                        upstream.cancel();
                        error(ex);
                    }
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
            T v = value;
            if (v != null) {
                value = null;
                complete(v);
            } else {
                complete();
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            upstream.cancel();
        }
    }
}
