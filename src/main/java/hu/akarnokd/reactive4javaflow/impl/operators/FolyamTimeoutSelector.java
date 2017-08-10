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

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamTimeoutSelector<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> firstTimeout;

    final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector;

    public FolyamTimeoutSelector(Folyam<T> source, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
        this.source = source;
        this.firstTimeout = firstTimeout;
        this.itemTimeoutSelector = itemTimeoutSelector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new TimeoutTimedSelectorConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, firstTimeout, itemTimeoutSelector));
        } else {
            source.subscribe(new TimeoutTimedSelectorSubscriber<>(s, firstTimeout, itemTimeoutSelector));
        }
    }

    static abstract class AbstractTimeoutTimedSelector<T> extends AtomicLong implements Flow.Subscription {

        final Flow.Publisher<?> firstTimeout;

        final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector;

        Flow.Subscription upstream;

        AutoDisposable task;
        static final VarHandle TASK;

        static {
            Lookup lk = lookup();
            try {
                TASK = lk.findVarHandle(AbstractTimeoutTimedSelector.class, "task", AutoDisposable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractTimeoutTimedSelector(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
            this.firstTimeout = firstTimeout;
            this.itemTimeoutSelector = itemTimeoutSelector;
        }


        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
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
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                upstream.cancel();
                DisposableHelper.dispose(this, TASK);
            }
        }

        final void timeout(long index) {
            if (get() == index && compareAndSet(index, Long.MIN_VALUE)) {
                upstream.cancel();
                TASK.setRelease(this, DisposableHelper.DISPOSED);
                error(new TimeoutException("Timeout awaiting item index: " + index));
            }
        }

        final void timeoutError(long index, Throwable ex) {
            if (get() == index && compareAndSet(index, Long.MIN_VALUE)) {
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

            long idx = getAcquire();
            if (idx != Long.MIN_VALUE && compareAndSet(idx, idx + 1)) {
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
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                error(throwable);
                DisposableHelper.dispose(this, TASK);
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        public final void onComplete() {
            if (getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                complete();
                DisposableHelper.dispose(this, TASK);
            }
        }

        abstract void error(Throwable ex);

        abstract void next(T item);

        abstract void complete();

        static final class TimeoutInnerSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

            final AbstractTimeoutTimedSelector parent;

            final long index;

            TimeoutInnerSubscriber(AbstractTimeoutTimedSelector parent, long index) {
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


    static final class TimeoutTimedSelectorSubscriber<T> extends AbstractTimeoutTimedSelector<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        protected TimeoutTimedSelectorSubscriber(FolyamSubscriber<? super T> actual, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
            super(firstTimeout, itemTimeoutSelector);
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

    static final class TimeoutTimedSelectorConditionalSubscriber<T> extends AbstractTimeoutTimedSelector<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        protected TimeoutTimedSelectorConditionalSubscriber(ConditionalSubscriber<? super T> actual, Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
            super(firstTimeout, itemTimeoutSelector);
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
    }

}
