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
package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public abstract class AbstractDisposableSubscriber<T> implements FolyamSubscriber<T>, AutoDisposable {

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    static {
        try {
            UPSTREAM = MethodHandles.lookup().findVarHandle(AbstractDisposableSubscriber.class, "upstream", Flow.Subscription.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    protected void onStart() {
        request(Long.MAX_VALUE);
    }

    protected final void request(long n) {
        if (n <= 0L) {
            FolyamPlugins.onError(new IllegalArgumentException("n <= 0: " + n));
        } else {
            upstream.request(n);
        }
    }

    @Override
    public final void onSubscribe(Flow.Subscription s) {
        if (SubscriptionHelper.replace(this, UPSTREAM, s)) {
            onStart();
        }
    }

    @Override
    public final void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
    }
}
