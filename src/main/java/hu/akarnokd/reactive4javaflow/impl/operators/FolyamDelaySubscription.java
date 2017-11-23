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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class FolyamDelaySubscription<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> other;

    public FolyamDelaySubscription(Folyam<T> source, Flow.Publisher<?> other) {
        this.source = source;
        this.other = other;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractDelaySubscription<T> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new DelaySubscriptionConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, source);
        } else {
            parent = new DelaySubscriptionSubscriber<>(s, source);
        }
        s.onSubscribe(parent);
        other.subscribe(parent);
    }

    static abstract class AbstractDelaySubscription<T> implements FolyamSubscriber<Object>, Flow.Subscription {

        FolyamPublisher<T> source;

        Flow.Subscription delayer;
        static final VarHandle DELAYER = VH.find(MethodHandles.lookup(), AbstractDelaySubscription.class, "delayer", Flow.Subscription.class);

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractDelaySubscription.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractDelaySubscription.class, "requested", long.class);

        boolean done;

        AbstractDelaySubscription(FolyamPublisher<T> source) {
            this.source = source;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, DELAYER, subscription)) {
                subscription.request(Long.MAX_VALUE);
            }
        }
        @Override
        public void onNext(Object item) {
            if (!done) {
                done = true;
                delayer.cancel();
                subscribeNext();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            error(throwable);
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                subscribeNext();
            }
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public void cancel() {
            SubscriptionHelper.cancel(this, DELAYER);
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        void subscribeNext() {
            delayer = SubscriptionHelper.CANCELLED;
            FolyamPublisher<T> source = this.source;
            this.source = null;
            source.subscribe(createInner());
        }

        void subscribeMain(Flow.Subscription s) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, s);
        }

        abstract FolyamSubscriber<T> createInner();

        abstract void error(Throwable ex);
    }

    static final class DelaySubscriptionSubscriber<T> extends AbstractDelaySubscription<T> {

        final FolyamSubscriber<? super T> actual;

        DelaySubscriptionSubscriber(FolyamSubscriber<? super T> actual, FolyamPublisher<T> source) {
            super(source);
            this.actual = actual;
        }

        @Override
        FolyamSubscriber<T> createInner() {
            return new InnerSubscriber(actual);
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        final class InnerSubscriber implements FolyamSubscriber<T> {

            final FolyamSubscriber<? super T> actual;

            InnerSubscriber(FolyamSubscriber<? super T> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscribeMain(subscription);
            }

            @Override
            public void onNext(T item) {
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

    static final class DelaySubscriptionConditionalSubscriber<T> extends AbstractDelaySubscription<T> {

        final ConditionalSubscriber<? super T> actual;

        DelaySubscriptionConditionalSubscriber(ConditionalSubscriber<? super T> actual, FolyamPublisher<T> source) {
            super(source);
            this.actual = actual;
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        FolyamSubscriber<T> createInner() {
            return new InnerSubscriber(actual);
        }

        final class InnerSubscriber implements ConditionalSubscriber<T> {

            final ConditionalSubscriber<? super T> actual;

            InnerSubscriber(ConditionalSubscriber<? super T> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscribeMain(subscription);
            }

            @Override
            public void onNext(T item) {
                actual.onNext(item);
            }

            @Override
            public boolean tryOnNext(T item) {
                return actual.tryOnNext(item);
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
