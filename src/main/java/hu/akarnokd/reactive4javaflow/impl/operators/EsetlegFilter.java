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
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.fused.*;

import java.util.concurrent.Flow;

public final class EsetlegFilter<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final CheckedPredicate<? super T> predicate;

    public EsetlegFilter(Esetleg<T> source, CheckedPredicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new FilterSubscriber<>(s, predicate));
    }

    static final class FilterSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final CheckedPredicate<? super T> predicate;

        FusedSubscription<T> qs;

        Flow.Subscription upstream;

        boolean done;

        FilterSubscriber(FolyamSubscriber<? super T> actual, CheckedPredicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }


        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            try {
                if (!predicate.test(item)) {
                    return;
                }
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                done = true;
                actual.onError(ex);
                return;
            }
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
            } else {
                actual.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (!done) {
                actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

        @Override
        public int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null && (mode & BOUNDARY) == 0) {
                return fs.requestFusion(mode);
            }
            return NONE;
        }

        @Override
        public T poll() throws Throwable {
            T v = qs.poll();
            if (v != null && predicate.test(v)) {
                return v;
            }
            return null;
        }

        @Override
        public boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public void clear() {
            qs.clear();
        }
    }
}
