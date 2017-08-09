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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamOnBackpressureDrop<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedConsumer<? super T> onDrop;

    public FolyamOnBackpressureDrop(Folyam<T> source, CheckedConsumer<? super T> onDrop) {
        this.source = source;
        this.onDrop = onDrop;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnBackpressureDropConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, onDrop));
        } else {
            source.subscribe(new OnBackpressureDropSubscriber<>(s, onDrop));
        }
    }

    static abstract class AbstractOnBackpressureDrop<T> extends AtomicLong implements FolyamSubscriber<T>, Flow.Subscription {

        final CheckedConsumer<? super T> onDrop;

        Flow.Subscription upstream;

        long emitted;

        boolean done;

        protected AbstractOnBackpressureDrop(CheckedConsumer<? super T> onDrop) {
            this.onDrop = onDrop;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            subscription.request(Long.MAX_VALUE);
        }

        abstract void onStart();

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, n);
        }

        @Override
        public final void cancel() {
            upstream.cancel();
        }
    }

    static final class OnBackpressureDropSubscriber<T> extends AbstractOnBackpressureDrop<T> {

        final FolyamSubscriber<? super T> actual;

        protected OnBackpressureDropSubscriber(FolyamSubscriber<? super T> actual, CheckedConsumer<? super T> onDrop) {
            super(onDrop);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            long e = emitted;
            long r = getAcquire();
            if (e != r) {
                actual.onNext(item);
                emitted = e + 1;
            } else {
                try {
                    onDrop.accept(item);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    upstream.cancel();
                    onError(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }

    static final class OnBackpressureDropConditionalSubscriber<T> extends AbstractOnBackpressureDrop<T> {

        final ConditionalSubscriber<? super T> actual;

        protected OnBackpressureDropConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedConsumer<? super T> onDrop) {
            super(onDrop);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            long e = emitted;
            long r = getAcquire();
            if (e != r) {
                if (actual.tryOnNext(item)) {
                    emitted = e + 1;
                }
            } else {
                try {
                    onDrop.accept(item);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    upstream.cancel();
                    onError(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
    }
}
