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

import java.util.NoSuchElementException;
import java.util.concurrent.Flow;

public final class FolyamSingle<T> extends Esetleg<T> {

    final Folyam<T> source;

    final boolean allowEmpty;

    public FolyamSingle(Folyam<T> source, boolean allowEmpty) {
        this.source = source;
        this.allowEmpty = allowEmpty;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new SingleSubscriber<>(s, allowEmpty));
    }

    static final class SingleSubscriber<T> extends DeferredScalarSubscription<T> implements FolyamSubscriber<T> {

        final boolean allowEmpty;

        Flow.Subscription upstream;

        public SingleSubscriber(FolyamSubscriber<? super T> actual, boolean allowEmpty) {
            super(actual);
            this.allowEmpty = allowEmpty;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            if (value != null) {
                value = null;
                upstream.cancel();
                error(new IndexOutOfBoundsException("The upstream has more than one item"));
            } else {
                value = item;
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
            value = null;
            if (v == null) {
                if (allowEmpty) {
                    complete();
                } else {
                    if (getAcquire() != CANCELLED) {
                        error(new NoSuchElementException("The upstream has no items"));
                    }
                }
            } else {
                complete(v);
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            upstream.cancel();
        }
    }
}
