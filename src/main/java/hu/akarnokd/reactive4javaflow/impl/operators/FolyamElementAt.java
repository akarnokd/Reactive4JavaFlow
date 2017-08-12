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

import java.util.concurrent.Flow;

public final class FolyamElementAt<T> extends Esetleg<T> {

    final Folyam<T> source;

    final long index;

    public FolyamElementAt(Folyam<T> source, long index) {
        this.source = source;
        this.index = index;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new ElementAtSubscriber<>(s, index));
    }

    static final class ElementAtSubscriber<T> extends DeferredScalarSubscription<T> implements FolyamSubscriber<T> {

        Flow.Subscription upstream;

        long index;

        public ElementAtSubscriber(FolyamSubscriber<? super T> actual, long index) {
            super(actual);
            this.index = index;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            long idx = index;
            if (idx == 0L) {
                upstream.cancel();
                complete(item);
            }
            index = idx - 1;
        }

        @Override
        public void onError(Throwable throwable) {
            error(throwable);
        }

        @Override
        public void onComplete() {
            if (index >= 0L) {
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
