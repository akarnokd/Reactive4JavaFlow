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

import java.util.concurrent.*;

public final class FolyamThrottleFirstTime<T> extends Folyam<T> {

    final Folyam<T> source;

    final long time;

    final TimeUnit unit;

    final SchedulerService executor;

    public FolyamThrottleFirstTime(Folyam<T> source, long time, TimeUnit unit, SchedulerService executor) {
        this.source = source;
        this.time = time;
        this.unit = unit;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new ThrottleFirstTimeConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, time, unit, executor));
        } else {
            source.subscribe(new ThrottleFirstTimeSubscriber<>(s, time, unit, executor));
        }
    }

    static final class ThrottleFirstTimeSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final long time;

        final TimeUnit unit;

        final SchedulerService executor;

        long lastTime;

        Flow.Subscription upstream;

        ThrottleFirstTimeSubscriber(FolyamSubscriber<? super T> actual, long time, TimeUnit unit, SchedulerService executor) {
            this.actual = actual;
            this.lastTime = Long.MIN_VALUE;
            this.time = time;
            this.unit = unit;
            this.executor = executor;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            long now = executor.now(unit);
            if (lastTime + time <= now) {
                lastTime = now;
                actual.onNext(item);
                return true;
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }


    static final class ThrottleFirstTimeConditionalSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final ConditionalSubscriber<? super T> actual;

        final long time;

        final TimeUnit unit;

        final SchedulerService executor;

        long lastTime;

        Flow.Subscription upstream;

        ThrottleFirstTimeConditionalSubscriber(ConditionalSubscriber<? super T> actual, long time, TimeUnit unit, SchedulerService executor) {
            this.actual = actual;
            this.lastTime = Long.MIN_VALUE;
            this.time = time;
            this.unit = unit;
            this.executor = executor;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            long now = executor.now(unit);
            if (lastTime + time <= now) {
                lastTime = now;
                return actual.tryOnNext(item);
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }
}
