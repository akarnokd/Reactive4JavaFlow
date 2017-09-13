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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscCountDownLatch;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class BlockingConsumerIgnore extends SpscCountDownLatch implements FolyamSubscriber<Object>, AutoDisposable {

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), BlockingConsumerIgnore.class, "upstream", Flow.Subscription.class);

    public BlockingConsumerIgnore() {
        super();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            subscription.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(Object item) {
        // deliberately ignored
    }

    @Override
    public void onError(Throwable throwable) {
        FolyamPlugins.onError(throwable);
        countDown();
    }

    @Override
    public void onComplete() {
        countDown();
    }

    @Override
    public void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
    }
}
