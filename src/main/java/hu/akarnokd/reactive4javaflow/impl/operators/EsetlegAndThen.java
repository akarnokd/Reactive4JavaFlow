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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consume the main source and ignore its values, then switch to a next
 * publisher and relay its values.
 * @param <T> the main source's value type
 * @param <U> the next source's value type
 * @since 0.1.2
 */
public final class EsetlegAndThen<T, U> extends Esetleg<U> {

    final FolyamPublisher<T> source;

    final FolyamPublisher<U> next;

    public EsetlegAndThen(FolyamPublisher<T> source, FolyamPublisher<U> next) {
        this.source = source;
        this.next = next;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super U> s) {
        AndThenMainSubscriber<U> parent = new AndThenMainSubscriber<>(s, next);
        s.onSubscribe(parent);
        parent.drain(source);
    }

    static final class AndThenMainSubscriber<U> extends AtomicInteger implements FolyamSubscriber<Object>, Flow.Subscription {

        final FolyamSubscriber<? super U> actual;

        Flow.Subscription mainUpstream;
        final static VarHandle MAIN_UPSTREAM = VH.find(MethodHandles.lookup(),
                AndThenMainSubscriber.class, "mainUpstream", Flow.Subscription.class);

        Flow.Subscription upstream;
        final static VarHandle UPSTREAM = VH.find(MethodHandles.lookup(),
                AndThenMainSubscriber.class, "upstream", Flow.Subscription.class);

        long requested;
        final static VarHandle REQUESTED = VH.find(MethodHandles.lookup(),
                AndThenMainSubscriber.class, "requested", long.class);

        Flow.Publisher<U> next;

        AndThenMainSubscriber(FolyamSubscriber<? super U> actual, Flow.Publisher<U> next) {
            this.actual = actual;
            this.next = next;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, MAIN_UPSTREAM, subscription)) {
                subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(Object item) {
            // ignored
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            drain(null);
        }

        void drain(FolyamPublisher<?> p) {
            if (getAndIncrement() == 0) {
                do {
                    if (p != null) {
                        p.subscribe(this);
                        p = null;
                    } else {
                        mainUpstream = SubscriptionHelper.CANCELLED;
                        Flow.Publisher<U> q = next;
                        next = null;
                        q.subscribe(new AndThenNextSubscriber<>(actual, this));
                        break;
                    }
                } while (decrementAndGet() != 0);
            }
        }

        void nextOnSubscribe(Flow.Subscription s) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, s);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public void cancel() {
            SubscriptionHelper.cancel(this, MAIN_UPSTREAM);
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        static final class AndThenNextSubscriber<U> implements FolyamSubscriber<U> {

            final FolyamSubscriber<? super U> actual;

            final AndThenMainSubscriber<U> parent;

            AndThenNextSubscriber(FolyamSubscriber<? super U> actual, AndThenMainSubscriber<U> parent) {
                this.actual = actual;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                parent.nextOnSubscribe(subscription);
            }

            @Override
            public void onNext(U item) {
                actual.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                actual.onError(throwable);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }
        }
    }
}
