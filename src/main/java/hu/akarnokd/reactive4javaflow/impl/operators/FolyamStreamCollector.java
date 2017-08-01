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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.*;
import java.util.stream.Collector;

public final class FolyamStreamCollector<T, A, R> extends Esetleg<R> {

    final Folyam<T> source;

    final Collector<T, A, R> collector;

    public FolyamStreamCollector(Folyam<T> source, Collector<T, A, R> collector) {
        this.source = source;
        this.collector = collector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        A initial;

        BiConsumer<A, T> acc;

        Function<A, R> fin;

        try {
            initial = Objects.requireNonNull(collector.supplier().get(), "The collector's supplier returned null");
            acc = Objects.requireNonNull(collector.accumulator(), "The collector's accumulator function is null");
            fin = Objects.requireNonNull(collector.finisher(), "The collector's finisher function is null");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        source.subscribe(new StreamCollectSubscriber<>(s, initial, acc, fin));
    }

    static final class StreamCollectSubscriber<T, A, R> extends DeferredScalarSubscription<R> implements FolyamSubscriber<T> {

        final BiConsumer<A, T> accumulator;

        final Function<A, R> finisher;

        Flow.Subscription upstream;

        A current;

        public StreamCollectSubscriber(FolyamSubscriber<? super R> actual, A initial, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
            super(actual);
            this.accumulator = accumulator;
            this.finisher = finisher;
            this.current = initial;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            A c = current;
            if (c != null) {
                try {
                    accumulator.accept(c, item);
                } catch (Throwable ex) {
                    current = null;
                    upstream.cancel();
                    error(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (current == null) {
                FolyamPlugins.onError(throwable);
                return;
            }
            current = null;
            value = null;
            error(throwable);
        }

        @Override
        public void onComplete() {
            A c = current;
            if (c != null) {
                current = null;
                R r;
                try {
                    r = Objects.requireNonNull(finisher.apply(c), "The collector's finisher returned a null value");
                } catch (Throwable ex) {
                    error(ex);
                    return;
                }
                complete(r);
            }
        }
    }
}
