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
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.util.Comparator;
import java.util.concurrent.Flow;

public final class EsetlegMinMax<T> extends Esetleg<T> {

    final Folyam<T> source;

    final Comparator<? super T> comparator;

    final int invert;

    public EsetlegMinMax(Folyam<T> source, Comparator<? super T> comparator, int invert) {
        this.source = source;
        this.comparator = comparator;
        this.invert = invert;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new MinMaxSubscriber<>(s, comparator, invert));
    }

    static final class MinMaxSubscriber<T> extends DeferredScalarSubscription<T> implements FolyamSubscriber<T> {

        final Comparator<? super T> comparator;

        final int invert;

        Flow.Subscription upstream;

        public MinMaxSubscriber(FolyamSubscriber<? super T> actual, Comparator<? super T> comparator, int invert) {
            super(actual);
            this.comparator = comparator;
            this.invert = invert;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {

            T v = value;
            if (v == null) {
                value = item;
            } else {
                int c;
                try {
                    c = invert * comparator.compare(v, item);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    upstream.cancel();
                    error(ex);
                    return;
                }
                if (c > 0) {
                    value = item;
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
                complete(v);
            } else {
                complete();
            }
        }
    }
}
