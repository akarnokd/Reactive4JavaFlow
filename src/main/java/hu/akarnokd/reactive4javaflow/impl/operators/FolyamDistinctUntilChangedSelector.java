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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.*;

import java.util.concurrent.Flow;

public final class FolyamDistinctUntilChangedSelector<T, K> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends K> keySelector;

    final CheckedBiPredicate<? super K, ? super K> comparator;

    public FolyamDistinctUntilChangedSelector(Folyam<T> source, CheckedFunction<? super T, ? extends K> keySelector, CheckedBiPredicate<? super K, ? super K> comparator) {
        this.source = source;
        this.keySelector = keySelector;
        this.comparator = comparator;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new DistinctUntilSelectorConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, keySelector, comparator));
        } else {
            source.subscribe(new DistinctUntilSelectorSubscriber<>(s, keySelector, comparator));
        }
    }

    static abstract class AbstractDistinctUntilChanged<T, K> implements ConditionalSubscriber<T>, FusedSubscription<T> {

        final CheckedFunction<? super T, ? extends K> keySelector;

        final CheckedBiPredicate<? super K, ? super K> comparator;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        K last;
        boolean hasLast;

        boolean done;

        int sourceFused;

        protected AbstractDistinctUntilChanged(CheckedFunction<? super T, ? extends K> keySelector, CheckedBiPredicate<? super K, ? super K> comparator) {
            this.keySelector = keySelector;
            this.comparator = comparator;
        }

        abstract void onStart();

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            onStart();
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
            FusedSubscription<T> fs = qs;
            for (;;) {
                T v = fs.poll();
                if (v == null) {
                    if (sourceFused == SYNC) {
                        last = null;
                    }
                    return null;
                }
                K key = keySelector.apply(v);
                if (!hasLast) {
                    hasLast = true;
                    last = key;
                    return v;
                }
                if (!comparator.test(last, key)) {
                    last = key;
                    return v;
                }
                last = key;
                if (sourceFused == ASYNC) {
                    upstream.request(1);
                }
            }
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public final void clear() {
            last = null;
            qs.clear();
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            upstream.cancel();
        }

        final boolean checkDuplicate(T item) {
            try {
                K key = keySelector.apply(item);
                boolean d;
                if (hasLast) {
                    d = comparator.test(last, key);
                } else {
                    hasLast = true;
                    d = false;
                }
                last = key;
                return d;
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return true;
            }
        }

        @Override
        public final void onNext(T item) {
            if (!tryOnNext(item) && !done) {
                upstream.request(1);
            }
        }
    }

    static final class DistinctUntilSelectorSubscriber<T, K> extends AbstractDistinctUntilChanged<T, K> {

        final FolyamSubscriber<? super T> actual;

        DistinctUntilSelectorSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super T, ? extends K> keySelector, CheckedBiPredicate<? super K, ? super K> comparator) {
            super(keySelector, comparator);
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
            if (!checkDuplicate(item)) {
                actual.onNext(item);
                return true;
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
            last = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            last = null;
            actual.onComplete();
        }
    }

    static final class DistinctUntilSelectorConditionalSubscriber<T, K> extends AbstractDistinctUntilChanged<T, K> {

        final ConditionalSubscriber<? super T> actual;

        DistinctUntilSelectorConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedFunction<? super T, ? extends K> keySelector, CheckedBiPredicate<? super K, ? super K> comparator) {
            super(keySelector, comparator);
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
            return !checkDuplicate(item) && actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            last = null;
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
