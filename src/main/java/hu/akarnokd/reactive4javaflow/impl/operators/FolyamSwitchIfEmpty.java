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
import hu.akarnokd.reactive4javaflow.impl.SubscriptionArbiter;

import java.util.concurrent.Flow;

public final class FolyamSwitchIfEmpty<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<? extends T> other;

    public FolyamSwitchIfEmpty(Folyam<T> source, Flow.Publisher<? extends T> other) {
        this.source = source;
        this.other = other;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new SwitchIfEmptyConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, other));
        } else {
            source.subscribe(new SwitchIfEmptySubscriber<>(s, other));
        }
    }

    static final class SwitchIfEmptySubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        Flow.Publisher<? extends T> other;

        boolean once;

        SwitchIfEmptySubscriber(FolyamSubscriber<? super T> actual, Flow.Publisher<? extends T> other) {
            this.actual = actual;
            this.other = other;
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
            if (!once) {
                once = true;
            }
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (once) {
                actual.onComplete();
            } else {
                once = true;
                Flow.Publisher<? extends T> p = this.other;
                other = null;
                p.subscribe(this);
            }
        }
    }

    static final class SwitchIfEmptyConditionalSubscriber<T> extends SubscriptionArbiter implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        Flow.Publisher<? extends T> other;

        boolean once;

        SwitchIfEmptyConditionalSubscriber(ConditionalSubscriber<? super T> actual, Flow.Publisher<? extends T> other) {
            this.actual = actual;
            this.other = other;
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
            if (!once) {
                once = true;
            }
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (!once) {
                once = true;
            }
            return actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (once) {
                actual.onComplete();
            } else {
                once = true;
                Flow.Publisher<? extends T> p = this.other;
                other = null;
                p.subscribe(this);
            }
        }
    }
}
