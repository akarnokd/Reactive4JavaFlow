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

public final class FolyamTakeUntil<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> other;

    public FolyamTakeUntil(Folyam<T> source, Flow.Publisher<?> other) {
        this.source = source;
        this.other = other;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            TakeUntilConditionalSubscriber<T> parent = new TakeUntilConditionalSubscriber<>((ConditionalSubscriber<? super T>)s);
            s.onSubscribe(parent);

            other.subscribe(parent.other);
            source.subscribe(parent);
        } else {
            TakeUntilSubscriber<T> parent = new TakeUntilSubscriber<>(s);
            s.onSubscribe(parent);

            other.subscribe(parent.other);
            source.subscribe(parent);
        }
    }

    static abstract class AbstractTakeUntil<T> implements Flow.Subscription {

        final UntilSubscriber other;

        int wip;
        static final VarHandle WIP;

        Throwable error;
        static final VarHandle ERROR;

        long requested;
        static final VarHandle REQUESTED;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(AbstractTakeUntil.class, "wip", Integer.TYPE);
                ERROR = MethodHandles.lookup().findVarHandle(AbstractTakeUntil.class, "error", Throwable.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractTakeUntil.class, "requested", Long.TYPE);
                UPSTREAM = MethodHandles.lookup().findVarHandle(AbstractTakeUntil.class, "upstream", Flow.Subscription.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractTakeUntil() {
            other = new UntilSubscriber(this);
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public final void cancel() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            SubscriptionHelper.cancel(other);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        abstract void otherSignal();

        abstract void otherError(Throwable ex);
    }

    static final class UntilSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object> {

        final AbstractTakeUntil<?> main;

        UntilSubscriber(AbstractTakeUntil<?> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, subscription)) {
                subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(Object item) {
            if (SubscriptionHelper.cancel(this)) {
                main.otherSignal();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                main.otherError(throwable);
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setRelease(SubscriptionHelper.CANCELLED);
                main.otherSignal();
            }
        }
    }

    static final class TakeUntilSubscriber<T> extends AbstractTakeUntil<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        TakeUntilSubscriber(FolyamSubscriber<? super T> actual) {
            super();
            this.actual = actual;
        }

        @Override
        void otherSignal() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }

        @Override
        void otherError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        public void onNext(T item) {
            HalfSerializer.onNext(actual, this, WIP, ERROR, item);
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

    static final class TakeUntilConditionalSubscriber<T> extends AbstractTakeUntil<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        TakeUntilConditionalSubscriber(ConditionalSubscriber<? super T> actual) {
            super();
            this.actual = actual;
        }

        @Override
        void otherSignal() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }

        @Override
        void otherError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        public void onNext(T item) {
            HalfSerializer.onNext(actual, this, WIP, ERROR, item);
        }

        @Override
        public boolean tryOnNext(T item) {
            return HalfSerializer.tryOnNext(actual, this, WIP, ERROR, item);
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
}
