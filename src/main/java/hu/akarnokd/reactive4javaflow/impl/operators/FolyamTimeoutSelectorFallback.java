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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class FolyamTimeoutSelectorFallback<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> firstTimeout;

    final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector;

    final Flow.Publisher<? extends T> fallback;

    public FolyamTimeoutSelectorFallback(Folyam<T> source, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
        this.source = source;
        this.firstTimeout = firstTimeout;
        this.itemTimeoutSelector = itemTimeoutSelector;
        this.fallback = fallback;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TimeoutTimedSelectorFallbackConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, firstTimeout, itemTimeoutSelector, fallback));
        } else {
            source.subscribe(new TimeoutTimedSelectorFallbackSubscriber<>(s, firstTimeout, itemTimeoutSelector, fallback));
        }
    }

    static abstract class AbstractTimeoutTimedSelectorFallback<T> extends SubscriptionArbiter implements Flow.Subscription {

        final Flow.Publisher<?> firstTimeout;

        final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector;

        final Flow.Publisher<? extends T> fallback;

        Flow.Subscription upstream;

        AutoDisposable task;
        static final VarHandle TASK;

        long index;
        static final VarHandle INDEX;

        static {
            MethodHandles.Lookup lk = MethodHandles.lookup();
            try {
                TASK = lk.findVarHandle(AbstractTimeoutTimedSelectorFallback.class, "task", AutoDisposable.class);
                INDEX = lk.findVarHandle(AbstractTimeoutTimedSelectorFallback.class, "index", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractTimeoutTimedSelectorFallback(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
            this.firstTimeout = firstTimeout;
            this.itemTimeoutSelector = itemTimeoutSelector;
            this.fallback = fallback;
        }


        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            arbiterReplace(subscription);
            if (firstTimeout != null) {
                AtomicDisposable d = new AtomicDisposable();
                task = d;

                onStart();
                if (d.get() == null) {
                    TimeoutInnerSubscriber inner = new TimeoutInnerSubscriber(this, 0);
                    if (d.compareAndSet(null, inner)) {
                        firstTimeout.subscribe(inner);
                    }
                }
            } else {
                onStart();
            }
        }

        abstract void onStart();

        @Override
        public final void cancel() {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                super.cancel();
                DisposableHelper.dispose(this, TASK);
            }
        }

        final void timeout(long index) {
            if ((long)INDEX.getAcquire(this) == index && INDEX.compareAndSet(this, index, Long.MIN_VALUE)) {
                upstream.cancel();
                TASK.setRelease(this, DisposableHelper.DISPOSED);
                if (index != 0L) {
                    arbiterProduced(index);
                }
                fallback.subscribe(createFallbackSubscriber());
            }
        }

        abstract FolyamSubscriber<T> createFallbackSubscriber();

        final void timeoutError(long index, Throwable ex) {
            if ((long)INDEX.getAcquire(this) == index && INDEX.compareAndSet(this, index, Long.MIN_VALUE)) {
                upstream.cancel();
                TASK.setRelease(this, DisposableHelper.DISPOSED);
                error(ex);
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        public final void onNext(T item) {
            AutoDisposable d = task;
            if (d != null) {
                d.close();
            }

            long idx = (long)INDEX.getAcquire(this);
            if (idx != Long.MIN_VALUE && INDEX.compareAndSet(this, idx, idx + 1)) {
                next(item);

                Flow.Publisher<?> p;

                try {
                    p = Objects.requireNonNull(itemTimeoutSelector.apply(item), "The itemTimeoutSelector returned a null Flow.Publisher");
                } catch (Throwable ex) {
                    onError(ex);
                    return;
                }
                // replace
                TimeoutInnerSubscriber inner = new TimeoutInnerSubscriber(this, idx + 1);
                if (DisposableHelper.replace(this, TASK, inner)) {
                    p.subscribe(inner);
                }
            }
        }

        public final void onError(Throwable throwable) {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                error(throwable);
                DisposableHelper.dispose(this, TASK);
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        public final void onComplete() {
            if ((long)INDEX.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
                complete();
                DisposableHelper.dispose(this, TASK);
            }
        }

        abstract void error(Throwable ex);

        abstract void next(T item);

        abstract void complete();

        static final class TimeoutInnerSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

            final AbstractTimeoutTimedSelectorFallback parent;

            final long index;

            TimeoutInnerSubscriber(AbstractTimeoutTimedSelectorFallback parent, long index) {
                this.parent = parent;
                this.index = index;
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

                    parent.timeout(index);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (getPlain() != SubscriptionHelper.CANCELLED) {
                    setPlain(SubscriptionHelper.CANCELLED);

                    parent.timeoutError(index, throwable);
                } else {
                    FolyamPlugins.onError(throwable);
                }
            }

            @Override
            public void onComplete() {
                if (getPlain() != SubscriptionHelper.CANCELLED) {
                    setPlain(SubscriptionHelper.CANCELLED);

                    parent.timeout(index);
                }
            }

            @Override
            public void close() {
                SubscriptionHelper.cancel(this);
            }
        }
    }

    static final class TimeoutTimedSelectorFallbackSubscriber<T> extends AbstractTimeoutTimedSelectorFallback<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        protected TimeoutTimedSelectorFallbackSubscriber(FolyamSubscriber<? super T> actual, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
            super(firstTimeout, itemTimeoutSelector, fallback);
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

    static final class TimeoutTimedSelectorFallbackConditionalSubscriber<T> extends AbstractTimeoutTimedSelectorFallback<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        protected TimeoutTimedSelectorFallbackConditionalSubscriber(ConditionalSubscriber<? super T> actual, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
            super(firstTimeout, itemTimeoutSelector, fallback);
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

            long idx = (long)INDEX.getAcquire(this);
            if (idx != Long.MIN_VALUE && INDEX.compareAndSet(this, idx, idx + 1)) {
                boolean b = actual.tryOnNext(item);

                Flow.Publisher<?> p;

                try {
                    p = Objects.requireNonNull(itemTimeoutSelector.apply(item), "The itemTimeoutSelector returned a null Flow.Publisher");
                } catch (Throwable ex) {
                    onError(ex);
                    return false;
                }
                // replace
                TimeoutInnerSubscriber inner = new TimeoutInnerSubscriber(this, idx + 1);
                if (DisposableHelper.replace(this, TASK, inner)) {
                    p.subscribe(inner);
                }
                return b;
            }
            return false;
        }

        @Override
        void complete() {
            actual.onComplete();
        }

        @Override
        FolyamSubscriber<T> createFallbackSubscriber() {
            return new FallbackSubscriber<>(this, actual);
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
