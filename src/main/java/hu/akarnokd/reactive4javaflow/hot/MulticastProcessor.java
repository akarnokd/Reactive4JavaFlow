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

package hu.akarnokd.reactive4javaflow.hot;

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;

import java.util.concurrent.Flow;

public final class MulticastProcessor<T> extends FolyamProcessor<T> {
    @Override
    public boolean hasThrowable() {
        return false;
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }

    @Override
    public boolean hasComplete() {
        return false;
    }

    @Override
    public boolean hasSubscribers() {
        return false;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {

    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

    }

    @Override
    public void onNext(T item) {

    }

    public boolean tryOnNext(T item) {
        return false;
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
}
