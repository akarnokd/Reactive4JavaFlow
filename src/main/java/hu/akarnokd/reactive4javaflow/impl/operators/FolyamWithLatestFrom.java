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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiFunction;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class FolyamWithLatestFrom<T, U, R> extends Folyam<R> {

    final Folyam<T> source;

    final Flow.Publisher<U> other;

    final CheckedBiFunction<? super T, ? super U, ? extends R> combiner;

    public FolyamWithLatestFrom(Folyam<T> source, Flow.Publisher<U> other, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        this.source = source;
        this.other = other;
        this.combiner = combiner;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        AbstractWithLatestFrom<T, U, R> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new WithLatestFromConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, combiner);
        } else {
            parent = new WithLatestFromSubscriber<>(s, combiner);
        }
        s.onSubscribe(parent);
        other.subscribe(parent.other);
        source.subscribe(parent);
    }

    static abstract class AbstractWithLatestFrom<T, U, R> implements ConditionalSubscriber<T>, Flow.Subscription {

        final CheckedBiFunction<? super T, ? super U, ? extends R> combiner;

        final OtherSubscriber<U> other;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractWithLatestFrom.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractWithLatestFrom.class, "requested", long.class);

        U latest;
        static final VarHandle LATEST = VH.find(MethodHandles.lookup(), AbstractWithLatestFrom.class, "latest", Object.class);

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractWithLatestFrom.class, "wip", int.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractWithLatestFrom.class, "error", Throwable.class);

        protected AbstractWithLatestFrom(CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
            this.combiner = combiner;
            this.other = new OtherSubscriber<>(this);
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public final void cancel() {
            upstream.cancel();
            SubscriptionHelper.cancel(other);
        }

        final void innerNext(U u) {
            LATEST.setRelease(this, u);
        }

        abstract void innerError(Throwable ex);

        abstract void innerComplete();

        static final class OtherSubscriber<U> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<U> {

            final AbstractWithLatestFrom<?, U, ?> parent;

            OtherSubscriber(AbstractWithLatestFrom<?, U, ?> parent) {
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (SubscriptionHelper.replace(this, subscription)) {
                    subscription.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(U item) {
                parent.innerNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerError(throwable);
            }

            @Override
            public void onComplete() {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerComplete();
            }
        }
    }

    static final class WithLatestFromSubscriber<T, U, R> extends AbstractWithLatestFrom<T, U, R> {

        final FolyamSubscriber<? super R> actual;

        protected WithLatestFromSubscriber(FolyamSubscriber<? super R> actual, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
            super(combiner);
            this.actual = actual;
        }

        @Override
        void innerError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        void innerComplete() {
            if (latest == null) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                HalfSerializer.onComplete(actual, this, WIP, ERROR);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if ((int)WIP.getAcquire(this) != 0) {
                return false;
            }
            U u = (U)LATEST.getAcquire(this);
            if (u != null) {
                R v;
                try {
                    v = Objects.requireNonNull(combiner.apply(item, u), "The combiner returned a null value");
                } catch (Throwable ex) {
                    upstream.cancel();
                    SubscriptionHelper.cancel(other);
                    onError(ex);
                    return false;
                }
                HalfSerializer.onNext(actual, this, WIP, ERROR, v);
                return true;
            }
            return false;
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item) && (int)WIP.getAcquire(this) == 0) {
                upstream.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }

    static final class WithLatestFromConditionalSubscriber<T, U, R> extends AbstractWithLatestFrom<T, U, R> {

        final ConditionalSubscriber<? super R> actual;

        protected WithLatestFromConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
            super(combiner);
            this.actual = actual;
        }

        @Override
        void innerError(Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        void innerComplete() {
            if (latest == null) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                HalfSerializer.onComplete(actual, this, WIP, ERROR);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if ((int)WIP.getAcquire(this) != 0) {
                return false;
            }
            U u = (U)LATEST.getAcquire(this);
            if (u != null) {
                R v;
                try {
                    v = Objects.requireNonNull(combiner.apply(item, u), "The combiner returned a null value");
                } catch (Throwable ex) {
                    upstream.cancel();
                    SubscriptionHelper.cancel(other);
                    onError(ex);
                    return false;
                }
                return HalfSerializer.tryOnNext(actual, this, WIP, ERROR, v);
            }
            return false;
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item) && (int)WIP.getAcquire(this) == 0) {
                upstream.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.cancel(other);
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }
}
