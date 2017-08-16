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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.processors.MulticastProcessor;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamPublish<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler;

    final int prefetch;

    public FolyamPublish(Folyam<T> source, CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler, int prefetch) {
        this.source = source;
        this.handler = handler;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        MulticastProcessor<T> mp = new MulticastProcessor<>(prefetch);
        Flow.Publisher<? extends R> p;
        try {
            p = Objects.requireNonNull(handler.apply(mp), "The handler returned a null Flow.Publisher");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            p.subscribe(new PublishConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mp));
        } else {
            p.subscribe(new PublishSubscriber<>(s, mp));
        }

        source.subscribe(mp);
    }

    static final class PublishSubscriber<R> implements FolyamSubscriber<R>, Flow.Subscription {

        final FolyamSubscriber<? super R> actual;

        final AutoDisposable mainSource;

        Flow.Subscription upstream;

        PublishSubscriber(FolyamSubscriber<? super R> actual, AutoDisposable mainSource) {
            this.actual = actual;
            this.mainSource = mainSource;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(R item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            mainSource.close();
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            mainSource.close();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            mainSource.close();
        }
    }


    static final class PublishConditionalSubscriber<R> implements ConditionalSubscriber<R>, Flow.Subscription {

        final ConditionalSubscriber<? super R> actual;

        final AutoDisposable mainSource;

        Flow.Subscription upstream;

        PublishConditionalSubscriber(ConditionalSubscriber<? super R> actual, AutoDisposable mainSource) {
            this.actual = actual;
            this.mainSource = mainSource;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(R item) {
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(R item) {
            return actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            mainSource.close();
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            mainSource.close();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            mainSource.close();
        }
    }
}
