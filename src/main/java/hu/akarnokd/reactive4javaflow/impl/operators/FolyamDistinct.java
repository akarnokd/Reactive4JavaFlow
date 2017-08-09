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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.*;
import java.util.concurrent.*;

public final class FolyamDistinct<T, K> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends K> keySelector;

    final Callable<? extends Collection<? super K>> collectionProvider;

    public FolyamDistinct(Folyam<T> source, CheckedFunction<? super T, ? extends K> keySelector, Callable<? extends Collection<? super K>> collectionProvider) {
        this.source = source;
        this.keySelector = keySelector;
        this.collectionProvider = collectionProvider;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Collection<? super K> collection;

        try {
            collection = Objects.requireNonNull(collectionProvider.call(), "The collectionProvider returned a null collection");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new DistinctConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, keySelector, collection));
        } else {
            source.subscribe(new DistinctSubscriber<>(s, keySelector, collection));
        }
    }

    static abstract class AbstractDistinct<T, K> implements ConditionalSubscriber<T>, FusedSubscription<T> {

        final CheckedFunction<? super T, ? extends K> keySelector;

        Collection<? super K> collection;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        boolean done;

        int sourceFused;

        AbstractDistinct(CheckedFunction<? super T, ? extends K> keySelector, Collection<? super K> collection) {
            this.keySelector = keySelector;
            this.collection = collection;
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
                return collection.add(key);
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return false;
            }
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
                        collection.clear();
                    }
                    return null;
                }
                if (collection.add(keySelector.apply(v))) {
                    return v;
                }
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
            qs.clear();
            collection.clear();
        }
    }

    static final class DistinctSubscriber<T, K> extends AbstractDistinct<T, K> {

        final FolyamSubscriber<? super T> actual;

        DistinctSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super T, ? extends K> keySelector, Collection<? super K> collection) {
            super(keySelector, collection);
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
            if (checkDuplicate(item)) {
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
            collection.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            collection.clear();
            actual.onComplete();
        }
    }

    static final class DistinctConditionalSubscriber<T, K> extends AbstractDistinct<T, K> {

        final ConditionalSubscriber<? super T> actual;

        DistinctConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedFunction<? super T, ? extends K> keySelector, Collection<? super K> collection) {
            super(keySelector, collection);
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
            return checkDuplicate(item) && actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            collection.clear();
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            collection.clear();
            actual.onComplete();
        }
    }
}
