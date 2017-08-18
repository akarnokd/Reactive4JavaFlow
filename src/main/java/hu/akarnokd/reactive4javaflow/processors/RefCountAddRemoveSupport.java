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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;

import java.util.concurrent.Flow;

interface RefCountAddRemoveSupport<T> {

    boolean add(RefCountSubscriber<T> ps);

    void remove(RefCountSubscriber<T> ps);

    final class RefCountSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final RefCountAddRemoveSupport<T> parent;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        RefCountSubscriber(FolyamSubscriber<? super T> actual, RefCountAddRemoveSupport<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            parent.remove(this);
        }

        @Override
        public int requestFusion(int mode) {
            FusedSubscription<T> fs = this.qs;
            return fs != null ? fs.requestFusion(mode) : NONE;
        }

        @Override
        public T poll() throws Throwable {
            return qs.poll();
        }

        @Override
        public boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public void clear() {
            qs.clear();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            parent.remove(this);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            parent.remove(this);
        }
    }

}
