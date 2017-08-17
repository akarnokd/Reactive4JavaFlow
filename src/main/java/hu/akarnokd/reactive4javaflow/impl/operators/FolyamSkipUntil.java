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
import java.util.concurrent.atomic.AtomicReference;

public final class FolyamSkipUntil<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> other;

    public FolyamSkipUntil(Folyam<T> source, Flow.Publisher<?> other) {
        this.source = source;
        this.other = other;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractSkipUntil<T> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new SkipUntilConditionalSubscriber<>((ConditionalSubscriber<? super T>)s);
        } else {
            parent = new SkipUntilSubscriber<>(s);
        }
        s.onSubscribe(parent);
        other.subscribe(parent.other);
        source.subscribe(parent);
    }

    static abstract class AbstractSkipUntil<T> implements Flow.Subscription, ConditionalSubscriber<T> {

        final SkipUntilOtherSubscriber other;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractSkipUntil.class, "wip", int.class);

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractSkipUntil.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractSkipUntil.class, "requested", long.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractSkipUntil.class, "error", Throwable.class);

        volatile boolean gate;

        protected AbstractSkipUntil() {
            other = new SkipUntilOtherSubscriber(this);
        }

        @Override
        public final void cancel() {
            SubscriptionHelper.cancel(other);
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        abstract void otherError(Throwable ex);
    }

    static final class SkipUntilSubscriber<T> extends AbstractSkipUntil<T> {

        final FolyamSubscriber<? super T> actual;

        SkipUntilSubscriber(FolyamSubscriber<? super T> actual) {
            super();
            this.actual = actual;
        }

        @Override
        public boolean tryOnNext(T item) {
            if (gate) {
                HalfSerializer.onNext(actual, this, WIP, ERROR, item);
                return true;
            }
            return false;
        }

        @Override
        void otherError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }


    static final class SkipUntilConditionalSubscriber<T> extends AbstractSkipUntil<T> {

        final ConditionalSubscriber<? super T> actual;

        SkipUntilConditionalSubscriber(ConditionalSubscriber<? super T> actual) {
            super();
            this.actual = actual;
        }

        @Override
        public boolean tryOnNext(T item) {
            return gate && HalfSerializer.tryOnNext(actual, this, WIP, ERROR, item);
        }

        @Override
        void otherError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }

    static final class SkipUntilOtherSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object> {

        final AbstractSkipUntil parent;

        SkipUntilOtherSubscriber(AbstractSkipUntil parent) {
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, subscription)) {
                subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(Object item) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                getPlain().cancel();
                setPlain(SubscriptionHelper.CANCELLED);
                parent.gate = true;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.otherError(throwable);
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.gate = true;
            }
        }
    }
}
