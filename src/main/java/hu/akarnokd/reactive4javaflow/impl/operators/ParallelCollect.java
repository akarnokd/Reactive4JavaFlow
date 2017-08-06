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

import java.util.Objects;
import java.util.concurrent.*;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiConsumer;
import hu.akarnokd.reactive4javaflow.impl.*;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <C> the collection type
 */
public final class ParallelCollect<T, C> extends ParallelFolyam<C> {

    final ParallelFolyam<T> source;

    final Callable<? extends C> initialCollection;

    final CheckedBiConsumer<? super C, ? super T> collector;

    public ParallelCollect(ParallelFolyam<T> source,
            Callable<? extends C> initialCollection, CheckedBiConsumer<? super C, ? super T> collector) {
        this.source = source;
        this.initialCollection = initialCollection;
        this.collector = collector;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super C>[] subscribers) {

        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        FolyamSubscriber<T>[] parents = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {

            C initialValue;

            try {
                initialValue = Objects.requireNonNull(initialCollection.call(), "The initialSupplier returned a null value");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                reportError(subscribers, ex);
                return;
            }

            parents[i] = new ParallelCollectSubscriber<T, C>(subscribers[i], initialValue, collector);
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

    static final class ParallelCollectSubscriber<T, C> extends DeferredScalarSubscription<C> implements FolyamSubscriber<T> {


        private static final long serialVersionUID = -4767392946044436228L;

        final CheckedBiConsumer<? super C, ? super T> collector;

        Flow.Subscription upstream;

        C collection;

        boolean done;

        ParallelCollectSubscriber(FolyamSubscriber<? super C> subscriber,
                C initialValue, CheckedBiConsumer<? super C, ? super T> collector) {
            super(subscriber);
            this.collection = initialValue;
            this.collector = collector;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.upstream = s;

            actual.onSubscribe(this);

            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }

            try {
                collector.accept(collection, t);
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                cancel();
                onError(ex);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            done = true;
            collection = null;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            C c = collection;
            collection = null;
            complete(c);
        }

        @Override
        public void cancel() {
            super.cancel();
            upstream.cancel();
        }
    }
}
