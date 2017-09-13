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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;

public final class EsetlegFlatMapIterable<T, R> extends Folyam<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper;

    public EsetlegFlatMapIterable(Esetleg<T> source, CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (!tryScalarXMap(source, s, mapper)) {
            if (s instanceof ConditionalSubscriber) {
                source.subscribe(new FlatMapIterableConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper));
            } else {
                source.subscribe(new FlatMapIterableSubscriber<>(s, mapper));
            }
        }
    }

    public static <T, R> boolean tryScalarXMap(FolyamPublisher<T> source, FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        if (source instanceof FusedDynamicSource) {
            FusedDynamicSource<T> f = (FusedDynamicSource<T>) source;
            Iterable<? extends R> e = null;

            try {
                T v = f.value();
                if (v != null) {
                    e = Objects.requireNonNull(mapper.apply(v), "The mapper returned a null Iterable");
                }
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                EmptySubscription.error(s, ex);
                return true;
            }

            if (e == null) {
                EmptySubscription.complete(s);
                return true;
            }

            Iterator<? extends R> it;
            boolean hasValue;

            try {
                it = e.iterator();
                hasValue = it.hasNext();
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                EmptySubscription.error(s, ex);
                return true;
            }

            if (hasValue) {
                if (s instanceof ConditionalSubscriber) {
                    s.onSubscribe(new FolyamIterable.IteratorConditionalSubscription<>((ConditionalSubscriber<? super R>)s, it));
                } else {
                    s.onSubscribe(new FolyamIterable.IteratorSubscription<>(s, it));
                }
            } else {
                EmptySubscription.complete(s);
            }

            return true;
        }
        return false;
    }

    static abstract class AbstractFlatMapIterableSubscriber<T, R> implements FolyamSubscriber<T>, FusedSubscription<R> {

        final CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractFlatMapIterableSubscriber.class, "upstream", Flow.Subscription.class);

        boolean outputFused;

        Iterator<? extends R> iterator;
        static final VarHandle ITERATOR = VH.find(MethodHandles.lookup(), AbstractFlatMapIterableSubscriber.class, "iterator", Iterator.class);

        boolean checkNext;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractFlatMapIterableSubscriber.class, "requested", long.class);

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractFlatMapIterableSubscriber.class, "wip", int.class);

        volatile boolean cancelled;

        boolean done;

        long emitted;

        protected AbstractFlatMapIterableSubscriber(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
            this.mapper = mapper;
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            if (ITERATOR.getAcquire(this) != null) {
                drain();
            }
        }

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onSubscribe();
            subscription.request(Long.MAX_VALUE);
        }

        abstract void onSubscribe();

        abstract void drain();


        @Override
        public final int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public final R poll() throws Throwable {
            Iterator<? extends R> it = (Iterator<? extends R>)ITERATOR.getAcquire(this);
            if (it != null) {
                if (checkNext) {
                    if (!it.hasNext()) {
                        iterator = null;
                        return null;
                    }
                } else {
                    checkNext = true;
                }
                return Objects.requireNonNull(it.next(), "The iterator returned a null item");
            }
            return null;
        }

        @Override
        public final boolean isEmpty() {
            return ITERATOR.getAcquire(this) == null;
        }

        @Override
        public final void clear() {
            ITERATOR.setRelease(this, null);
        }
    }

    static final class FlatMapIterableSubscriber<T, R> extends AbstractFlatMapIterableSubscriber<T, R> {

        final FolyamSubscriber<? super R> actual;

        FlatMapIterableSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
            super(mapper);
            this.actual = actual;
        }

        @Override
        void onSubscribe() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            Iterator<? extends R> p;
            boolean hasValue;
            try {
                p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Iterable")
                        .iterator();
                hasValue = p.hasNext();
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                done = true;
                actual.onError(ex);
                return;
            }
            if (hasValue) {
                done = true;
                ITERATOR.setRelease(this, p);
                drain();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
            } else {
                actual.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (!done) {
                actual.onComplete();
            }
        }

        @Override
        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            FolyamSubscriber<? super R> a = actual;
            Iterator<? extends R> it = (Iterator<? extends R>)ITERATOR.getAcquire(this);

            if (outputFused && it != null) {
                a.onNext(null);
                a.onComplete();
                return;
            }

            int missed = 1;
            long e = emitted;

            for (;;) {

                if (it != null) {

                    long r = (long) REQUESTED.getAcquire(this);

                    if (r == Long.MAX_VALUE) {
                        fastPath(a, it);
                        return;
                    }

                    while (e != r) {
                        if (cancelled) {
                            return;
                        }

                        R v;
                        try {
                            v = Objects.requireNonNull(it.next(), "The iterator returned a null item");
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            a.onError(ex);
                            return;
                        }

                        a.onNext(v);

                        if (cancelled) {
                            return;
                        }

                        boolean b;
                        try {
                            b = it.hasNext();
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            a.onError(ex);
                            return;
                        }

                        if (cancelled) {
                            return;
                        }

                        if (!b) {
                            a.onComplete();
                            return;
                        }

                        e++;
                    }

                    emitted = e;
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
                if (it == null) {
                    it = (Iterator<? extends R>)ITERATOR.getAcquire(this);
                }
            }
        }

        void fastPath(FolyamSubscriber<? super R> a, Iterator<? extends R> it) {
            for (;;) {
                if (cancelled) {
                    return;
                }

                R v;
                try {
                    v = Objects.requireNonNull(it.next(), "The iterator returned a null item");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }

                a.onNext(v);

                if (cancelled) {
                    return;
                }

                boolean b;
                try {
                    b = it.hasNext();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }

                if (!b) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }
            }
        }
    }


    static final class FlatMapIterableConditionalSubscriber<T, R>  extends AbstractFlatMapIterableSubscriber<T, R> {

        final ConditionalSubscriber<? super R> actual;

        FlatMapIterableConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
            super(mapper);
            this.actual = actual;
        }

        @Override
        void onSubscribe() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            Iterator<? extends R> p;
            boolean hasValue;
            try {
                p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Iterable")
                        .iterator();
                hasValue = p.hasNext();
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                done = true;
                actual.onError(ex);
                return;
            }
            if (hasValue) {
                done = true;
                ITERATOR.setRelease(this, p);
                drain();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
            } else {
                actual.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (!done) {
                actual.onComplete();
            }
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            ConditionalSubscriber<? super R> a = actual;
            Iterator<? extends R> it = (Iterator<? extends R>)ITERATOR.getAcquire(this);

            if (outputFused && it != null) {
                a.onNext(null);
                a.onComplete();
                return;
            }

            int missed = 1;
            long e = emitted;

            for (;;) {

                if (it != null) {

                    long r = (long) REQUESTED.getAcquire(this);

                    if (r == Long.MAX_VALUE) {
                        fastPath(a, it);
                        return;
                    }

                    while (e != r) {
                        if (cancelled) {
                            return;
                        }

                        R v;
                        try {
                            v = Objects.requireNonNull(it.next(), "The iterator returned a null item");
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            a.onError(ex);
                            return;
                        }

                        if (a.tryOnNext(v)) {
                            e++;
                        }

                        if (cancelled) {
                            return;
                        }

                        boolean b;
                        try {
                            b = it.hasNext();
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            a.onError(ex);
                            return;
                        }

                        if (cancelled) {
                            return;
                        }

                        if (!b) {
                            a.onComplete();
                            return;
                        }
                    }

                    emitted = e;
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
                if (it == null) {
                    it = (Iterator<? extends R>)ITERATOR.getAcquire(this);
                }
            }
        }

        void fastPath(ConditionalSubscriber<? super R> a, Iterator<? extends R> it) {
            for (;;) {
                if (cancelled) {
                    return;
                }

                R v;
                try {
                    v = Objects.requireNonNull(it.next(), "The iterator returned a null item");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }

                a.tryOnNext(v);

                if (cancelled) {
                    return;
                }

                boolean b;
                try {
                    b = it.hasNext();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    return;
                }

                if (!b) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }
            }
        }
    }
}
