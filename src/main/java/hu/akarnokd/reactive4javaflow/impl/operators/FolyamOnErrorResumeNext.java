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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionArbiter;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamOnErrorResumeNext<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler;

    public FolyamOnErrorResumeNext(Folyam<T> source, CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
        this.source = source;
        this.handler = handler;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnErrorResumeNextConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, handler));
        } else {
            source.subscribe(new OnErrorResumeNextSubscriber<>(s, handler));
        }
    }

    static final class OnErrorResumeNextSubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        final CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler;

        long produced;

        boolean once;

        OnErrorResumeNextSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
            this.actual = actual;
            this.handler = handler;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
            if (!once) {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (once) {
                actual.onError(throwable);
                return;
            }
            once = true;
            Flow.Publisher<? extends T> p;

            try {
                p = Objects.requireNonNull(handler.apply(throwable), "The handler returned a null Flow.Publisher");
            } catch (Throwable ex) {
                actual.onError(new CompositeThrowable(throwable, ex));
                return;
            }

            long c = produced;
            if (c != 0) {
                arbiterProduced(c);
            }
            p.subscribe(this);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }

    static final class OnErrorResumeNextConditionalSubscriber<T> extends SubscriptionArbiter implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        final CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler;

        long produced;

        boolean once;

        OnErrorResumeNextConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
            this.actual = actual;
            this.handler = handler;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
            if (!once) {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (actual.tryOnNext(item)) {
                produced++;
                return true;
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            if (once) {
                actual.onError(throwable);
                return;
            }
            once = true;
            Flow.Publisher<? extends T> p;

            try {
                p = Objects.requireNonNull(handler.apply(throwable), "The handler returned a null Flow.Publisher");
            } catch (Throwable ex) {
                actual.onError(new CompositeThrowable(throwable, ex));
                return;
            }

            long c = produced;
            if (c != 0) {
                arbiterProduced(c);
            }
            p.subscribe(this);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
}
