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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;

import java.util.concurrent.Flow;

public final class FolyamIgnoreElements<T> extends Esetleg<T> {

    final Folyam<T> source;

    public FolyamIgnoreElements(Folyam<T> source) {
        this.source = source;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new IgnoreElementsSubscriber<>(s));
    }

    static final class IgnoreElementsSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        Flow.Subscription upstream;

        IgnoreElementsSubscriber(FolyamSubscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            // elements are deliberately ignored
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
        public int requestFusion(int mode) {
            return mode & ASYNC;
        }

        @Override
        public T poll() throws Throwable {
            return null;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public void clear() {
            // deliberately no op
        }

        @Override
        public void request(long n) {
            // ignored; values are never produced
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }
}
