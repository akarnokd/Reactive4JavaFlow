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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscCountDownLatch;

import java.lang.invoke.*;
import java.util.concurrent.*;

public abstract class AbstractBlockingConsumer<T> extends SpscCountDownLatch implements FolyamSubscriber<T> {

    T item;
    Throwable error;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractBlockingConsumer.class, "upstream", Flow.Subscription.class);

    public AbstractBlockingConsumer() {
        super();
    }

    @Override
    public final void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            subscription.request(Long.MAX_VALUE);
        }
    }

    protected final void cancel() {
        SubscriptionHelper.cancel(this, UPSTREAM);
    }

    public final T blockingGet() {
        if (getCount() != 0) {
            try {
                await();
            } catch (InterruptedException ex) {
                cancel();
                throw new RuntimeException(ex);
            }
        }
        Throwable ex = error;
        if (ex != null) {
            if (ex instanceof Error) {
                throw (Error)ex;
            }
            if (ex instanceof RuntimeException) {
                throw (RuntimeException)ex;
            }
            throw new RuntimeException(ex);
        }
        return item;
    }

    public final T blockingGet(long time, TimeUnit unit) {
        if (getCount() != 0) {
            try {
                if (!await(time, unit)) {
                    cancel();
                    throw new TimeoutException();
                }
            } catch (InterruptedException | TimeoutException ex) {
                cancel();
                throw new RuntimeException(ex);
            }
        }
        Throwable ex = error;
        if (ex != null) {
            if (ex instanceof Error) {
                throw (Error)ex;
            }
            if (ex instanceof RuntimeException) {
                throw (RuntimeException)ex;
            }
            throw new RuntimeException(ex);
        }
        return item;
    }
}
