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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;

import java.util.concurrent.Flow;

public final class SafeFolyamSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription {

    final FolyamSubscriber<? super T> actual;

    Flow.Subscription upstream;

    boolean done;

    public SafeFolyamSubscriber(FolyamSubscriber<? super T> actual) {
        this.actual = actual;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        upstream = subscription;
        try {
            actual.onSubscribe(this);
        } catch (Throwable ex) {
            done = true;
            cancel();
            FolyamPlugins.onError(ex);
        }
    }

    @Override
    public void onNext(T item) {
        if (done) {
            return;
        }

        try {
            actual.onNext(item);
        } catch (Throwable ex) {
            cancel();
            onError(ex);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (done) {
            FolyamPlugins.onError(throwable);
            return;
        }
        done = true;
        try {
            actual.onError(throwable);
        } catch (Throwable ex) {
            FolyamPlugins.onError(new CompositeThrowable(throwable, ex));
        }
    }

    @Override
    public void onComplete() {
        if (done) {
            return;
        }
        done = true;
        try {
            actual.onComplete();
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
        }
    }

    @Override
    public void request(long n) {
        try {
            upstream.request(n);
        } catch (Throwable ex) {
            cancel();
            FolyamPlugins.onError(ex);
        }
    }

    @Override
    public void cancel() {
        try {
            upstream.cancel();
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
        }
    }
}
