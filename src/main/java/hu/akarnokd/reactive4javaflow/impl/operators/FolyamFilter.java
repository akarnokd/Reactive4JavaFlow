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

public final class FolyamFilter<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedPredicate<? super T> predicate;

    public FolyamFilter(Folyam<T> source, CheckedPredicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FilterConditionalSubscriber<>((ConditionalSubscriber<? super T>) s, predicate));
        } else {
            source.subscribe(new FilterSubscriber<>(s, predicate));
        }
    }

    static abstract class AbstractFilterSubscriber<T> implements ConditionalSubscriber<T>, FusedSubscription<T> {

        final CheckedPredicate<? super T> predicate;

        boolean done;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        int fusionMode;

        protected AbstractFilterSubscriber(CheckedPredicate<? super T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public final void onNext(T item) {
            if (!tryOnNext(item) && !done) {
                upstream.request(1);
            }
        }

        @Override
        public final T poll() throws Throwable {
            FusedSubscription<T> qs = this.qs;
            for (;;) {
                T v = qs.poll();
                if (v == null) {
                    return null;
                }

                if (predicate.test(v)) {
                    return v;
                }
                if (fusionMode == ASYNC) {
                    qs.request(1);
                }
            }
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @Override
        public final int requestFusion(int mode) {
            if (qs != null && (mode & BOUNDARY) == 0) {
                int m = qs.requestFusion(mode);
                fusionMode = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;

            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription) subscription;
            }

            start();
        }

        abstract void start();

        @Override
        public final void cancel() {
            upstream.cancel();
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }
    }

    static final class FilterSubscriber<T> extends AbstractFilterSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        FilterSubscriber(FolyamSubscriber<? super T> actual, CheckedPredicate<? super T> predicate) {
            super(predicate);
            this.actual = actual;
        }

        @Override
        void start() {
            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (done) {
                return false;
            }

            if (item == null) {
                actual.onNext(null);
                return true;
            }

            boolean b;
            try {
                b = predicate.test(item);
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return false;
            }
            if (b) {
                actual.onNext(item);
            }
            return b;
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }

    static final class FilterConditionalSubscriber<T> extends AbstractFilterSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        FilterConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedPredicate<? super T> predicate) {
            super(predicate);
            this.actual = actual;
        }

        @Override
        void start() {
            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (done) {
                return false;
            }

            if (item == null) {
                return actual.tryOnNext(null);
            }

            boolean b;
            try {
                b = predicate.test(item);
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return false;
            }
            if (b) {
                return actual.tryOnNext(item);
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }
}
