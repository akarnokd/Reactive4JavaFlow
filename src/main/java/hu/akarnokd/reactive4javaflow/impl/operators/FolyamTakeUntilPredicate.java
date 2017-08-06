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

public final class FolyamTakeUntilPredicate<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedPredicate<? super T> predicate;

    public FolyamTakeUntilPredicate(Folyam<T> source, CheckedPredicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TakeUntilPredicateConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, predicate));
        } else {
            source.subscribe(new TakeUntilPredicateSubscriber<>(s, predicate));
        }
    }

    static abstract class AbstractTakeUntilPredicate<T> implements ConditionalSubscriber<T>, FusedSubscription<T> {

        final CheckedPredicate<? super T> predicate;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        boolean done;

        boolean delayDone;

        int sourceFused;

        protected AbstractTakeUntilPredicate(CheckedPredicate<? super T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

        @Override
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null && (mode & BOUNDARY) == 0) {
                int m = fs.requestFusion(mode);
                sourceFused = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final T poll() throws Throwable {
            if (done) {
                return null;
            }
            if (delayDone) {
                onComplete();
                return null;
            }
            T v = qs.poll();

            if (v != null) {
                if (predicate.test(v)) {
                    if (sourceFused == ASYNC) {
                        delayDone = true;
                    } else {
                        done = true;
                    }
                }
            }

            return v;
        }

        @Override
        public final boolean isEmpty() {
            return done || qs.isEmpty();
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            onStart();
        }

        abstract void onStart();

        @Override
        public final void onNext(T item) {
            if (!tryOnNext(item) && !done) {
                upstream.request(1);
            }
        }
    }

    static final class TakeUntilPredicateSubscriber<T> extends AbstractTakeUntilPredicate<T> {

        final FolyamSubscriber<? super T> actual;

        protected TakeUntilPredicateSubscriber(FolyamSubscriber<? super T> actual, CheckedPredicate<? super T> predicate) {
            super(predicate);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (item == null) {
                actual.onNext(null);
                return true;
            }
            actual.onNext(item);

            boolean b;
            try {
                b = predicate.test(item);
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return false;
            }
            if (b) {
                upstream.cancel();
                onComplete();
            }
            return true;
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

    static final class TakeUntilPredicateConditionalSubscriber<T> extends AbstractTakeUntilPredicate<T> {

        final ConditionalSubscriber<? super T> actual;

        protected TakeUntilPredicateConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedPredicate<? super T> predicate) {
            super(predicate);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (item == null) {
                return actual.tryOnNext(null);
            }

            boolean c = actual.tryOnNext(item);

            boolean b;
            try {
                b = predicate.test(item);
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return false;
            }
            if (b) {
                upstream.cancel();
                onComplete();
            }
            return c;
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
