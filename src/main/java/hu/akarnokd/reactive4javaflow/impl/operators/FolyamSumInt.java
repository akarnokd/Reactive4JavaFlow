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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamSumInt<T> extends Esetleg<Integer> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Number> valueSelector;

    public FolyamSumInt(Folyam<T> source, CheckedFunction<? super T, ? extends Number> valueSelector) {
        this.source = source;
        this.valueSelector = valueSelector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
        source.subscribe(new SumIntSubscriber<>(s, valueSelector));
    }


    static final class SumIntSubscriber<T> extends DeferredScalarSubscription<Integer> implements FolyamSubscriber<T> {

        final CheckedFunction<? super T, ? extends Number> valueSelector;

        Flow.Subscription upstream;

        int sum;
        boolean hasValue;

        SumIntSubscriber(FolyamSubscriber<? super Integer> actual, CheckedFunction<? super T, ? extends Number> valueSelector) {
            super(actual);
            this.valueSelector = valueSelector;
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
                if (!hasValue) {
                    hasValue = true;
                }
                try {
                    sum += Objects.requireNonNull(valueSelector.apply(item), "The valueSelector returned a null Number").intValue();
                } catch (Throwable ex) {
                    upstream.cancel();
                    error(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            error(throwable);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                complete(sum);
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
