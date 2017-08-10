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
import hu.akarnokd.reactive4javaflow.impl.AtomicDisposable;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamTimeoutTimed<T> extends Folyam<T> {

    final Folyam<T> source;

    final long timeout;

    final TimeUnit unit;

    final SchedulerService executor;

    public FolyamTimeoutTimed(Folyam<T> source, long timeout, TimeUnit unit, SchedulerService executor) {
        this.source = source;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TimeoutTimedConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, timeout, unit, executor.worker()));
        } else {
            source.subscribe(new TimeoutTimedSubscriber<>(s, timeout, unit, executor.worker()));
        }
    }

    static abstract class AbstractTimeoutTimed<T> extends AtomicLong implements Flow.Subscription {

        final long timeout;

        final TimeUnit unit;

        final SchedulerService.Worker worker;

        Flow.Subscription upstream;

        AutoDisposable task;

        protected AbstractTimeoutTimed(long timeout, TimeUnit unit, SchedulerService.Worker worker) {
            this.timeout = timeout;
            this.unit = unit;
            this.worker = worker;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            AtomicDisposable d = new AtomicDisposable();
            task = d;
            onStart();
            if (d.get() == null) {
                d.replace(worker.schedule(() -> timeout(0L), timeout, unit));
            }
        }

        abstract void onStart();

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                upstream.cancel();
                worker.close();
            }
        }

        final void timeout(long index) {
            if (get() == index && compareAndSet(index, Long.MIN_VALUE)) {
                upstream.cancel();
                error(new TimeoutException("Timeout awaiting item index: " + index));
                worker.close();
            }
        }

        public final void onNext(T item) {
            AutoDisposable d = task;
            if (d != null) {
                d.close();
            }

            long idx = getAcquire();
            if (idx != Long.MIN_VALUE && compareAndSet(idx, idx + 1)) {
                next(item);

                task = worker.schedule(() -> timeout(idx + 1), timeout, unit);
            }
        }

        public final void onError(Throwable throwable) {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                error(throwable);
                task = null;
                worker.close();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        public final void onComplete() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                complete();
                task = null;
                worker.close();
            }
        }

        abstract void error(Throwable ex);

        abstract void next(T item);

        abstract void complete();
    }

    static final class TimeoutTimedSubscriber<T> extends AbstractTimeoutTimed<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        protected TimeoutTimedSubscriber(FolyamSubscriber<? super T> actual, long timeout, TimeUnit unit, SchedulerService.Worker worker) {
            super(timeout, unit, worker);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
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
    }

    static final class TimeoutTimedConditionalSubscriber<T> extends AbstractTimeoutTimed<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        protected TimeoutTimedConditionalSubscriber(ConditionalSubscriber<? super T> actual, long timeout, TimeUnit unit, SchedulerService.Worker worker) {
            super(timeout, unit, worker);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
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

            long idx = getAcquire();
            if (idx != Long.MIN_VALUE && compareAndSet(idx, idx + 1)) {
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
    }
}
