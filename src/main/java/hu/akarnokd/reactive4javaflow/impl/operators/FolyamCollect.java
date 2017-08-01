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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiConsumer;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.Objects;
import java.util.concurrent.*;

public final class FolyamCollect<T, C> extends Esetleg<C> {

    final Folyam<T> source;

    final Callable<C> collectionSupplier;

    final CheckedBiConsumer<C, ? super T> collector;

    public FolyamCollect(Folyam<T> source, Callable<C> collectionSupplier, CheckedBiConsumer<C, ? super T> collector) {
        this.source = source;
        this.collectionSupplier = collectionSupplier;
        this.collector = collector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super C> s) {

        C initial;

        try {
            initial = Objects.requireNonNull(collectionSupplier.call(), "The collectionSupplier returned a null value");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        source.subscribe(new CollectSubscriber<>(s, initial, collector));
    }

    static final class CollectSubscriber<T, C> extends DeferredScalarSubscription<C> implements FolyamSubscriber<T> {

        final CheckedBiConsumer<C,? super T> collector;

        Flow.Subscription upstream;

        public CollectSubscriber(FolyamSubscriber<? super C> actual, C initial, CheckedBiConsumer<C, ? super T> collector) {
            super(actual);
            this.collector = collector;
            this.value = initial;
        }


        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            C c = value;
            if (c != null) {
                try {
                    collector.accept(c, item);
                } catch (Throwable ex) {
                    value = null;
                    upstream.cancel();
                    error(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (value == null) {
                FolyamPlugins.onError(throwable);
                return;
            }
            value = null;
            error(throwable);
        }

        @Override
        public void onComplete() {
            C c = value;
            if (c != null) {
                value = null;
                complete(c);
            }
        }
    }
}
