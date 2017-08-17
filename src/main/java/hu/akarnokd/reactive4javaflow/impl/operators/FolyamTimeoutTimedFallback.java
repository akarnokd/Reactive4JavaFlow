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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.concurrent.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamTimeoutTimedFallback<T> extends Folyam<T> {

    final Folyam<T> source;

    final long timeout;

    final TimeUnit unit;

    final SchedulerService executor;

    final Flow.Publisher<? extends T> fallback;

    public FolyamTimeoutTimedFallback(Folyam<T> source, long timeout, TimeUnit unit, SchedulerService executor, Flow.Publisher<? extends T> fallback) {
        this.source = source;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
        this.fallback = fallback;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TimeoutTimedFallbackConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, timeout, unit, executor.worker(), fallback));
        } else {
            source.subscribe(new TimeoutTimedFallbackSubscriber<>(s, timeout, unit, executor.worker(), fallback));
        }

    }
    static abstract class AbstractTimeoutTimedFallback<T> extends SubscriptionArbiter {

        final long timeout;

        final TimeUnit unit;

        final SchedulerService.Worker worker;

        final Flow.Publisher<? extends T> fallback;

        Flow.Subscription upstream;

        AutoDisposable task;

        long index;
        static final VarHandle INDEX = VH.find(MethodHandles.lookup(), AbstractTimeoutTimedFallback.class, "index", long.class);

        protected AbstractTimeoutTimedFallback(long timeout, TimeUnit unit, SchedulerService.Worker worker, Flow.Publisher<? extends T> fallback) {
            this.timeout = timeout;
            this.unit = unit;
            this.worker = worker;
            this.fallback = fallback;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            arbiterReplace(subscription);
            AtomicDisposable d = new AtomicDisposable();
            task = d;
            onStart();
            if (d.get() == null) {
                d.replace(worker.schedule(() -> timeout(0L), timeout, unit));
            }
        }

        abstract void onStart();

        @Override
        public final void cancel() {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                super.cancel();
                worker.close();
            }
        }

        final void timeout(long index) {
            if ((long)INDEX.getAcquire(this) == index && INDEX.compareAndSet(this, index, Long.MIN_VALUE)) {
                upstream.cancel();

                if (index != 0L) {
                    arbiterProduced(index);
                }
                fallback.subscribe(createFallbackSubscriber());
                worker.close();
            }
        }

        abstract FolyamSubscriber<T> createFallbackSubscriber();

        public final void onNext(T item) {
            AutoDisposable d = task;
            if (d != null) {
                d.close();
            }

            long idx = (long)INDEX.getAcquire(this);
            if (idx != Long.MIN_VALUE && INDEX.compareAndSet(this, idx, idx + 1)) {
                next(item);

                task = worker.schedule(() -> timeout(idx + 1), timeout, unit);
            }
        }

        public final void onError(Throwable throwable) {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                error(throwable);
                task = null;
                worker.close();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        public final void onComplete() {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                complete();
                task = null;
                worker.close();
            }
        }

        abstract void error(Throwable ex);

        abstract void next(T item);

        abstract void complete();
    }

    static final class TimeoutTimedFallbackSubscriber<T> extends AbstractTimeoutTimedFallback<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        protected TimeoutTimedFallbackSubscriber(FolyamSubscriber<? super T> actual, long timeout, TimeUnit unit, SchedulerService.Worker worker, Flow.Publisher<? extends T> fallback) {
            super(timeout, unit, worker, fallback);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        FolyamSubscriber<T> createFallbackSubscriber() {
            return new FallbackSubscriber<>(this, actual);
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        void next(T item) {
            actual.onNext(item);
        }

        @Override
        void complete() {
            actual.onComplete();
        }

        static final class FallbackSubscriber<T> implements FolyamSubscriber<T> {

            final SubscriptionArbiter arbiter;

            final FolyamSubscriber<? super T> actual;

            FallbackSubscriber(SubscriptionArbiter arbiter, FolyamSubscriber<? super T> actual) {
                this.arbiter = arbiter;
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                arbiter.arbiterReplace(subscription);
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

    static final class TimeoutTimedFallbackConditionalSubscriber<T> extends AbstractTimeoutTimedFallback<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        protected TimeoutTimedFallbackConditionalSubscriber(ConditionalSubscriber<? super T> actual, long timeout, TimeUnit unit, SchedulerService.Worker worker, Flow.Publisher<? extends T> fallback) {
            super(timeout, unit, worker, fallback);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        FolyamSubscriber<T> createFallbackSubscriber() {
            return new FallbackSubscriber<>(this, actual);
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        void next(T item) {
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            AutoDisposable d = task;
            if (d != null) {
                d.close();
            }

            long idx = (long)INDEX.getAcquire(this);
            if (idx != Long.MIN_VALUE && INDEX.compareAndSet(this, idx, idx + 1)) {
                boolean b = actual.tryOnNext(item);

                task = worker.schedule(() -> timeout(idx + 1), timeout, unit);

                return b;
            }
            return false;
        }

        @Override
        void complete() {
            actual.onComplete();
        }

        static final class FallbackSubscriber<T> implements ConditionalSubscriber<T> {

            final SubscriptionArbiter arbiter;

            final ConditionalSubscriber<? super T> actual;

            FallbackSubscriber(SubscriptionArbiter arbiter, ConditionalSubscriber<? super T> actual) {
                this.arbiter = arbiter;
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                arbiter.arbiterReplace(subscription);
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
