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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class FolyamWithLatestFromMany<T, U, R> extends Folyam<R> {

    final Folyam<T> source;

    final Iterable<? extends Flow.Publisher<? extends U>> other;

    final CheckedFunction<? super Object[], ? extends R> combiner;

    public FolyamWithLatestFromMany(Folyam<T> source, Iterable<? extends Flow.Publisher<? extends U>> other, CheckedFunction<? super Object[], ? extends R> combiner) {
        this.source = source;
        this.other = other;
        this.combiner = combiner;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        Flow.Publisher<? extends U>[] p = new Flow.Publisher[8];
        int n = 0;
        try {
            for (Flow.Publisher<? extends U> u : other) {
                if (n == p.length) {
                    p = Arrays.copyOf(p, n + (n >> 2));
                }
                p[n++] = Objects.requireNonNull(u, "The iterator returned a null Flow.Publisher");
            }
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }
        AbstractWithLatestFromMany<T, U, R> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new WithLatestFromManyConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, combiner, n);
        } else {
            parent = new WithLatestFromManySubscriber<>(s, combiner, n);
        }
        s.onSubscribe(parent);
        parent.subscribe(p, n);
        source.subscribe(parent);
    }


    static abstract class AbstractWithLatestFromMany<T, U, R> implements ConditionalSubscriber<T>, Flow.Subscription {

        final CheckedFunction<? super Object[], ? extends R> combiner;

        final OtherSubscriber<U>[] other;

        final int n;

        int active;
        static final VarHandle ACTIVE = VH.find(MethodHandles.lookup(), AbstractWithLatestFromMany.class, "active", int.class);

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractWithLatestFromMany.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractWithLatestFromMany.class, "requested", long.class);

        Object[] latest;
        static final VarHandle LATEST = MethodHandles.arrayElementVarHandle(Object[].class);

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractWithLatestFromMany.class, "wip", int.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractWithLatestFromMany.class, "error", Throwable.class);

        protected AbstractWithLatestFromMany(CheckedFunction<? super Object[], ? extends R> combiner, int n) {
            this.combiner = combiner;
            other = new OtherSubscriber[n];
            for (int i = 0; i < n; i++) {
                other[i] = new OtherSubscriber<>(this, i);
            }
            latest = new Object[n];
            this.n = n;
        }

        void subscribe(Flow.Publisher<? extends U>[] sources, int n) {
            OtherSubscriber<U>[] other = this.other;
            for (int i = 0; i < n; i++) {
                sources[i].subscribe(other[i]);
            }
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
            cancelAll();
        }

        final void cancelAll() {
            for (OtherSubscriber<U> o : other) {
                SubscriptionHelper.cancel(o);
            }
        }

        final void innerNext(int index, U u) {
            LATEST.setRelease(latest, index, u);
        }

        abstract void innerError(int index, Throwable ex);

        abstract void innerComplete(int index);

        final void cancelExcept(int index) {
            OtherSubscriber<U>[] other = this.other;
            int n = other.length;
            for (int i = 0; i < index; i++) {
                SubscriptionHelper.cancel(other[i]);
            }
            for (int i = index + 1; i < n; i++) {
                SubscriptionHelper.cancel(other[i]);
            }
        }

        void makeActive() {
            ACTIVE.getAndAdd(this, 1);
        }

        static final class OtherSubscriber<U> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<U> {

            final AbstractWithLatestFromMany<?, U, ?> parent;

            final int index;

            boolean once;

            OtherSubscriber(AbstractWithLatestFromMany<?, U, ?> parent, int index) {
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
            public void onNext(U item) {
                if (!once) {
                    once = true;
                    parent.makeActive();
                }
                parent.innerNext(index, item);
            }

            @Override
            public void onError(Throwable throwable) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerError(index, throwable);
            }

            @Override
            public void onComplete() {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerComplete(index);
            }
        }
    }

    static final class WithLatestFromManySubscriber<T, U, R> extends AbstractWithLatestFromMany<T, U, R> {

        final FolyamSubscriber<? super R> actual;

        protected WithLatestFromManySubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> combiner, int n) {
            super(combiner, n);
            this.actual = actual;
        }

        @Override
        void innerError(int index, Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            cancelExcept(index);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        void innerComplete(int index) {
            if (latest[index] == null) {
                Arrays.fill(latest, null);
                SubscriptionHelper.cancel(this, UPSTREAM);
                cancelExcept(index);
                HalfSerializer.onComplete(actual, this, WIP, ERROR);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if ((int)WIP.getAcquire(this) != 0) {
                return false;
            }

            int n = this.n;

            if ((int)ACTIVE.getAcquire(this) == n) {
                Object[] latest = this.latest;
                Object[] o = new Object[n + 1];
                o[0] = item;
                for (int i = 0; i < n; i++) {
                    o[i + 1] = LATEST.getAcquire(latest, i);
                }
                R v;
                try {
                    v = Objects.requireNonNull(combiner.apply(o), "The combiner returned a null value");
                } catch (Throwable ex) {
                    upstream.cancel();
                    cancelAll();
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
            Arrays.fill(latest, null);
            cancelAll();
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            Arrays.fill(latest, null);
            cancelAll();
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }

    static final class WithLatestFromManyConditionalSubscriber<T, U, R> extends AbstractWithLatestFromMany<T, U, R> {

        final ConditionalSubscriber<? super R> actual;

        protected WithLatestFromManyConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> combiner, int n) {
            super(combiner, n);
            this.actual = actual;
        }

        @Override
        void innerError(int index, Throwable ex) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            cancelExcept(index);
            HalfSerializer.onError(actual, this, WIP, ERROR, ex);
        }

        @Override
        void innerComplete(int index) {
            if (latest[index] == null) {
                Arrays.fill(latest, null);
                SubscriptionHelper.cancel(this, UPSTREAM);
                cancelExcept(index);
                HalfSerializer.onComplete(actual, this, WIP, ERROR);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if ((int)WIP.getAcquire(this) != 0) {
                return false;
            }

            int n = this.n;

            if ((int)ACTIVE.getAcquire(this) == n) {
                Object[] latest = this.latest;
                Object[] o = new Object[n + 1];
                o[0] = item;
                for (int i = 0; i < n; i++) {
                    o[i + 1] = LATEST.getAcquire(latest, i);
                }
                R v;
                try {
                    v = Objects.requireNonNull(combiner.apply(o), "The combiner returned a null value");
                } catch (Throwable ex) {
                    upstream.cancel();
                    cancelAll();
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
            Arrays.fill(latest, null);
            cancelAll();
            HalfSerializer.onError(actual, this, WIP, ERROR, throwable);
        }

        @Override
        public void onComplete() {
            Arrays.fill(latest, null);
            cancelAll();
            HalfSerializer.onComplete(actual, this, WIP, ERROR);
        }
    }
}
