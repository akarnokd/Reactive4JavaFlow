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

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <R> the result value type
 */
public final class ParallelReduce<T, R> extends ParallelFolyam<R> {

    final ParallelFolyam<T> source;

    final Callable<? extends R> initialSupplier;

    final CheckedBiFunction<R, ? super T, R> reducer;

    public ParallelReduce(ParallelFolyam<T> source, Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> reducer) {
        this.source = source;
        this.initialSupplier = initialSupplier;
        this.reducer = reducer;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super R>[] subscribers) {

        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        FolyamSubscriber<T>[] parents = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {

            R initialValue;

            try {
                initialValue = Objects.requireNonNull(initialSupplier.call(), "The initialSupplier returned a null value");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                reportError(subscribers, ex);
                return;
            }

            parents[i] = new ParallelReduceSubscriber<T, R>(subscribers[i], initialValue, reducer);
        }

        source.subscribe(parents);
    }

    void reportError(FolyamSubscriber<?>[] subscribers, Throwable ex) {
        for (FolyamSubscriber<?> s : subscribers) {
            EmptySubscription.error(s, ex);
        }
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    static final class ParallelReduceSubscriber<T, R> extends DeferredScalarSubscription<R> implements FolyamSubscriber<T> {


        private static final long serialVersionUID = 8200530050639449080L;

        final CheckedBiFunction<R, ? super T, R> reducer;

        Flow.Subscription s;

        R accumulator;

        boolean done;

        ParallelReduceSubscriber(FolyamSubscriber<? super R> subscriber, R initialValue, CheckedBiFunction<R, ? super T, R> reducer) {
            super(subscriber);
            this.accumulator = initialValue;
            this.reducer = reducer;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);

            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            if (!done) {
                R v;

                try {
                    v = Objects.requireNonNull(reducer.apply(accumulator, t), "The reducer returned a null value");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    cancel();
                    onError(ex);
                    return;
                }

                accumulator = v;
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            done = true;
            accumulator = null;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;

                R a = accumulator;
                accumulator = null;
                complete(a);
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
