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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.functionals.CheckedRunnable;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class LambdaSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<T>, AutoDisposable {

    final CheckedConsumer<? super T> onNext;

    final CheckedConsumer<? super Throwable> onError;

    final CheckedRunnable onComplete;

    final CheckedConsumer<? super Flow.Subscription> onSubscribe;

    boolean done;

    public LambdaSubscriber(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError, CheckedRunnable onComplete, CheckedConsumer<? super Flow.Subscription> onSubscribe) {
        this.onNext = onNext;
        this.onError = onError;
        this.onComplete = onComplete;
        this.onSubscribe = onSubscribe;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, subscription)) {
            try {
                onSubscribe.accept(subscription);
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                subscription.cancel();
                onError(ex);
            }
        }
    }

    @Override
    public void onNext(T item) {
        if (done) {
            return;
        }

        try {
            onNext.accept(item);
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            getPlain().cancel();
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
            onError.accept(throwable);
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            ex.addSuppressed(throwable);
            FolyamPlugins.onError(ex);
        }
    }

    @Override
    public void onComplete() {
        if (done) {
            return;
        }
        done = true;
        try {
            onComplete.run();
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            FolyamPlugins.onError(ex);
        }
    }

    @Override
    public void close() {
        SubscriptionHelper.cancel(this);
    }
}
