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

public final class FolyamDebounce<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer;

    public FolyamDebounce(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer) {
        this.source = source;
        this.itemDebouncer = itemDebouncer;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new DebounceSubscriber<>(s, itemDebouncer));
    }

    static final class DebounceSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer;

        Flow.Subscription upstream;

        long index;
        static final VarHandle INDEX = VH.find(MethodHandles.lookup(), DebounceSubscriber.class, "index", long.class);

        AutoDisposable task;
        static final VarHandle TASK = VH.find(MethodHandles.lookup(), DebounceSubscriber.class, "task", AutoDisposable.class);

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), DebounceSubscriber.class, "wip", int.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), DebounceSubscriber.class, "error", Throwable.class);

        DebounceSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer) {
            this.actual = actual;
            this.itemDebouncer = itemDebouncer;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1L);
            }
        }

        @Override
        public boolean tryOnNext(T item) {

            boolean b = false;

            long idx = (long)INDEX.getAcquire(this);

            if (idx != Long.MAX_VALUE && INDEX.compareAndSet(this, idx, idx + 1)) {
                AutoDisposable d = task;
                if (d != null && d != DisposableHelper.CLOSED) {
                    task = null;
                    b = !SubscriptionHelper.cancel((ItemSubscriber<?>)d);
                }

                Flow.Publisher<?> p;
                try {
                    p = Objects.requireNonNull(itemDebouncer.apply(item), "The itemDebouncer returned a null Flow.Publisher");
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return false;
                }

                ItemSubscriber<T> inner = new ItemSubscriber<>(idx + 1, item, this);
                if (DisposableHelper.replace(this, TASK, inner)) {
                    p.subscribe(inner);
                }
            }

            return b;
        }

        @Override
        public void onError(Throwable throwable) {
            if ((long)INDEX.getAndSet(this, Long.MAX_VALUE) != Long.MAX_VALUE) {
                AutoDisposable d = task;
                if (d != null) {
                    d.close();
                }
                HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
            }
        }

        @Override
        public void onComplete() {
            if ((long)INDEX.getAndSet(this, Long.MAX_VALUE) != Long.MAX_VALUE) {
                AutoDisposable d = task;
                if (d == null || d == DisposableHelper.CLOSED || !((ItemSubscriber<?>) d).emitLast()) {
                    HalfSerializer.onComplete(actual, this, WIP, ERROR);
                }
            }
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            if ((long)INDEX.getAndSet(this, Long.MAX_VALUE) != Long.MAX_VALUE) {
                upstream.cancel();
                DisposableHelper.close(this, TASK);
            }
        }

        void itemSignal(long index, T item) {
            if ((long)INDEX.getAcquire(this) == index) {
                HalfSerializer.onNext(actual, this, WIP, ERROR, item);
            }
        }

        void itemError(long index, Throwable throwable) {
            if ((long)INDEX.getAndSet(this, Long.MAX_VALUE) != Long.MAX_VALUE) {
                upstream.cancel();
                HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
            }
        }

        void itemSignalLast(T item) {
            HalfSerializer.onNext(actual, this, WIP, ERROR, item);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }

        static final class ItemSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

            final long index;

            final T item;

            final DebounceSubscriber<T> parent;

            ItemSubscriber(long index, T item, DebounceSubscriber<T> parent) {
                this.index = index;
                this.item = item;
                this.parent = parent;
            }

            @Override
            public void close() {
                SubscriptionHelper.cancel(this);
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (SubscriptionHelper.replace(this, subscription)) {
                    subscription.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(Object item) {
                if (SubscriptionHelper.cancel(this)) {
                    parent.itemSignal(index, this.item);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                Flow.Subscription s = getAcquire();
                if (s != SubscriptionHelper.CANCELLED && compareAndSet(s, SubscriptionHelper.CANCELLED)) {
                    parent.itemError(index, throwable);
                }
            }

            @Override
            public void onComplete() {
                Flow.Subscription s = getAcquire();
                if (s != SubscriptionHelper.CANCELLED && compareAndSet(s, SubscriptionHelper.CANCELLED)) {
                    parent.itemSignal(index, this.item);
                }
            }

            boolean emitLast() {
                Flow.Subscription s = getAcquire();
                if (s != SubscriptionHelper.CANCELLED && compareAndSet(s, SubscriptionHelper.CANCELLED)) {
                    parent.itemSignalLast(this.item);
                    return true;
                }
                return false;
            }
        }
    }
}
