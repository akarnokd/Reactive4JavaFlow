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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.hot.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamRetryWhen<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler;

    public FolyamRetryWhen(Folyam<T> source, CheckedFunction<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler) {
        this.source = source;
        this.handler = handler;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        FolyamProcessor<Throwable> signaller = new DirectProcessor<Throwable>().toSerialized();
        Flow.Publisher<?> p;

        try {
            p = Objects.requireNonNull(handler.apply(signaller), "The handler returned a null Flow.Publisher");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        AbstractRetryWhen<T> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new RetryWhenConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, source, signaller);
        } else {
            parent = new RetryWhenSubscriber<>(s, source, signaller);
        }

        s.onSubscribe(parent);
        p.subscribe(parent.responder);
        parent.next();
    }

    static abstract class AbstractRetryWhen<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final FolyamPublisher<T> source;

        final FolyamSubscriber<Throwable> signaller;

        final HandlerSubscriber responder;

        int wipEmission;
        static final VarHandle WIP_EMISSION;

        Throwable error;
        static final VarHandle ERROR;

        int wipAgain;
        static final VarHandle WIP_AGAIN;

        long produced;

        static {
            try {
                WIP_EMISSION = MethodHandles.lookup().findVarHandle(AbstractRetryWhen.class, "wipEmission", int.class);
                WIP_AGAIN = MethodHandles.lookup().findVarHandle(AbstractRetryWhen.class, "wipAgain", int.class);
                ERROR = MethodHandles.lookup().findVarHandle(AbstractRetryWhen.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractRetryWhen(FolyamPublisher<T> source, FolyamSubscriber<Throwable> signaller) {
            this.source = source;
            this.responder = new HandlerSubscriber(this);
            this.signaller = signaller;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        @Override
        public final void onError(Throwable throwable) {
            responder.request(1);
            signaller.onNext(throwable);
        }

        @Override
        public final void cancel() {
            super.cancel();
            responder.cancel();
        }

        final void next() {
            if ((int) WIP_AGAIN.getAndAdd(this, 1) == 0) {
                do {
                    if (arbiterIsCancelled()) {
                        return;
                    }
                    long p = produced;
                    if (p != 0L) {
                        produced = 0;
                        arbiterProduced(p);
                    }
                    source.subscribe(this);
                } while ((int)WIP_AGAIN.getAndAdd(this, -1) - 1 != 0);
            }
        }

        abstract void error(Throwable ex);

        abstract void complete();
    }

    static final class RetryWhenSubscriber<T> extends AbstractRetryWhen<T> {

        final FolyamSubscriber<? super T> actual;

        RetryWhenSubscriber(FolyamSubscriber<? super T> actual, FolyamPublisher<T> source, FolyamSubscriber<Throwable> signaller) {
            super(source, signaller);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            HalfSerializer.onNext(actual, this, WIP_EMISSION, ERROR, item);
        }

        @Override
        public void onComplete() {
            responder.cancel();
            HalfSerializer.onComplete(actual, this, WIP_EMISSION, ERROR);
        }

        void complete() {
            HalfSerializer.onComplete(actual, this, WIP_EMISSION, ERROR);
        }

        void error(Throwable ex) {
            super.cancel();
            HalfSerializer.onError(actual, this, WIP_EMISSION, ERROR, ex);
        }
    }

    static final class RetryWhenConditionalSubscriber<T> extends AbstractRetryWhen<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        RetryWhenConditionalSubscriber(ConditionalSubscriber<? super T> actual, FolyamPublisher<T> source, FolyamSubscriber<Throwable> signaller) {
            super(source, signaller);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            HalfSerializer.onNext(actual, this, WIP_EMISSION, ERROR, item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (HalfSerializer.tryOnNext(actual, this, WIP_EMISSION, ERROR, item)) {
                produced++;
                return true;
            }
            return false;
        }

        @Override
        public void onComplete() {
            responder.cancel();
            HalfSerializer.onComplete(actual, this, WIP_EMISSION, ERROR);
        }

        void complete() {
            super.cancel();
            HalfSerializer.onComplete(actual, this, WIP_EMISSION, ERROR);
        }

        void error(Throwable ex) {
            super.cancel();
            HalfSerializer.onError(actual, this, WIP_EMISSION, ERROR, ex);
        }
    }

    static final class HandlerSubscriber implements FolyamSubscriber<Object>, Flow.Subscription {

        final AbstractRetryWhen<?> parent;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        static {
            try {
                REQUESTED = MethodHandles.lookup().findVarHandle(HandlerSubscriber.class, "requested", long.class);
                UPSTREAM = MethodHandles.lookup().findVarHandle(HandlerSubscriber.class, "upstream", Flow.Subscription.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        HandlerSubscriber(AbstractRetryWhen<?> parent) {
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public void onNext(Object item) {
            parent.next();
        }

        @Override
        public void onError(Throwable throwable) {
            parent.error(throwable);
        }

        @Override
        public void onComplete() {
            parent.complete();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public void cancel() {
            SubscriptionHelper.cancel(this, UPSTREAM);
        }
    }

}
