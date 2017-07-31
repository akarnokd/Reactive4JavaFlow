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

import java.util.concurrent.Flow;

public final class FolyamHide<T> extends Folyam<T> {

    final Flow.Publisher<T> source;

    public FolyamHide(Flow.Publisher<T> source) {
        this.source = source;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new HideSubscriber<>(s));
    }

    static final class HideSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        Flow.Subscription upstream;

        HideSubscriber(FolyamSubscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }

}
