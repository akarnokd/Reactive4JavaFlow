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

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.lang.invoke.*;
import java.util.concurrent.*;

public final class CompletionStageConsumer<T> extends CompletableFuture<T> implements FolyamSubscriber<T> {

    T item;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    static {
        try {
            UPSTREAM = MethodHandles.lookup().findVarHandle(CompletionStageConsumer.class, "upstream", Flow.Subscription.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            whenComplete(this::cancel);
            subscription.request(Long.MAX_VALUE);
        }
    }

    void cancel(T t, Throwable e) {
        SubscriptionHelper.cancel(this, UPSTREAM);
        item = null;
    }

    @Override
    public void onNext(T item) {
        this.item = item;
    }

    @Override
    public void onError(Throwable throwable) {
        item = null;
        super.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        super.complete(item);
    }
}
