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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamMapWhen<T, U, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper;

    final CheckedBiFunction<? super T, ? super U, ? extends R> combiner;

    final int prefetch;

    final boolean delayError;

    public FolyamMapWhen(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch, boolean delayError) {
        this.source = source;
        this.mapper = mapper;
        this.combiner = combiner;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new MapWhenConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, combiner, prefetch, delayError));
        } else {
            source.subscribe(new MapWhenSubscriber<>(s, mapper, combiner, prefetch, delayError));
        }
    }

    static abstract class AbstractMapWhen<T, U, R> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription {

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper;

        final CheckedBiFunction<? super T, ? super U, ? extends R> combiner;

        final int prefetch;

        final boolean delayError;

        final T[] array;

        Flow.Subscription upstream;

        AutoDisposable other;
        static final VarHandle OTHER;

        long producerIndex;
        static final VarHandle PRODUCER_INDEX;

        long consumerIndex;
        static final VarHandle CONSUMER_INDEX;

        long requested;
        static final VarHandle REQUESTED;

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE;

        static final VarHandle ARRAY;

        U result;
        int state;
        static final VarHandle STATE;

        static final int STATE_NONE = 0;
        static final int STATE_RUNNING = 1;
        static final int STATE_EMPTY = 2;
        static final int STATE_VALUE = 3;

        Throwable error;
        static final VarHandle ERROR;

        long emitted;

        int consumed;

        static {
            Lookup lk = lookup();
            try {
                PRODUCER_INDEX = lk.findVarHandle(AbstractMapWhen.class, "producerIndex", long.class);
                CONSUMER_INDEX = lk.findVarHandle(AbstractMapWhen.class, "consumerIndex", long.class);
                DONE = lk.findVarHandle(AbstractMapWhen.class, "done", boolean.class);
                STATE = lk.findVarHandle(AbstractMapWhen.class, "state", int.class);
                ERROR = lk.findVarHandle(AbstractMapWhen.class, "error", Throwable.class);
                REQUESTED = lk.findVarHandle(AbstractMapWhen.class, "requested", long.class);
                OTHER = lk.findVarHandle(AbstractMapWhen.class, "other", AutoDisposable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        }


        AbstractMapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch, boolean delayError) {
            this.mapper = mapper;
            this.combiner = combiner;
            this.prefetch = prefetch;
            this.delayError = delayError;
            this.array = (T[])new Object[QueueHelper.pow2(prefetch)];
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            subscription.request(prefetch);
        }

        @Override
        public void onNext(T item) {
            T[] a = array;
            int m = a.length - 1;
            long pi = producerIndex;
            int offset = (int)pi & m;
            ARRAY.setRelease(a, offset, item);
            PRODUCER_INDEX.setRelease(this, pi + 1);
            drain();
        }

        @Override
        public void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                if (!delayError) {
                    DisposableHelper.dispose(this, OTHER);
                }
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            upstream.cancel();
            DisposableHelper.dispose(this, OTHER);
            if (getAndIncrement() == 0) {
                result = null;
                Arrays.fill(array, null);
            }
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        final void innerNext(U item) {
            result = item;
            STATE.setRelease(this, STATE_VALUE);
            drain();
        }

        final void innerError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                if (!delayError) {
                    upstream.cancel();
                }
                STATE.setRelease(this, STATE_EMPTY);
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        final void innerComplete() {
            STATE.setRelease(this, STATE_EMPTY);
            drain();
        }

        abstract void drainLoop();

        abstract void onStart();
    }

    static final class MapWhenSubscriber<T, U, R> extends AbstractMapWhen<T, U, R> {

        final FolyamSubscriber<? super R> actual;

        MapWhenSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch, boolean delayError) {
            super(mapper, combiner, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            FolyamSubscriber<? super R> a = actual;
            Object[] arr = array;
            int m = arr.length - 1;
            int missed = 1;
            long e = emitted;
            int c = consumed;
            long ci = consumerIndex;
            int lim = prefetch - (prefetch >> 2);
            CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper = this.mapper;
            CheckedBiFunction<? super T, ? super U, ? extends R> combiner = this.combiner;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        result = null;
                        Arrays.fill(arr, null);
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        result = null;
                        Arrays.fill(arr, null);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    int offset = (int)ci & m;
                    T t = (T)ARRAY.getAcquire(arr, offset);
                    boolean empty = t == null;

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (e == r || empty) {
                        break;
                    }

                    int s = (int)STATE.getAcquire(this);
                    if (s == STATE_NONE) {

                        Flow.Publisher<? extends U> p;
                        try {
                            p = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null Flow.Publisher");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            result = null;
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        if (p instanceof FusedDynamicSource) {
                            FusedDynamicSource<U> fs = (FusedDynamicSource<U>) p;
                            STATE.setRelease(this, STATE_NONE);
                            ARRAY.setRelease(array, offset, null);
                            U u;

                            try {
                                u = fs.value();
                            } catch (Throwable ex) {
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayError) {
                                    upstream.cancel();
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    Arrays.fill(arr, null);
                                    a.onError(ex);
                                    return;
                                }
                                u = null;
                            }

                            if (u != null) {
                                R w;

                                try {
                                    w = Objects.requireNonNull(combiner.apply(t, u), "The combiner returned a null value");
                                } catch (Throwable ex) {
                                    ExceptionHelper.addThrowable(this, ERROR, ex);
                                    upstream.cancel();
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    result = null;
                                    Arrays.fill(arr, null);
                                    a.onError(ex);
                                    return;
                                }

                                a.onNext(w);
                                e++;
                            }

                            ci++;
                            if (++c == lim) {
                                c = 0;
                                upstream.request(lim);
                            }
                        } else {
                            MapWhenInnerSubscriber<U> inner = new MapWhenInnerSubscriber<>(this);
                            if (DisposableHelper.replace(this, OTHER, inner)) {
                                STATE.setRelease(this, STATE_RUNNING);
                                p.subscribe(inner);
                            } else {
                                return;
                            }
                        }
                    } else if (s == STATE_VALUE) {
                        STATE.setRelease(this, STATE_NONE);
                        ARRAY.setRelease(array, offset, null);
                        U u = result;
                        result = null;
                        R w;

                        try {
                            w = Objects.requireNonNull(combiner.apply(t, u), "The combiner returned a null value");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            result = null;
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        a.onNext(w);

                        e++;
                        ci++;
                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }
                    } else if (s == STATE_EMPTY) {
                        STATE.setRelease(this, STATE_NONE);
                        ARRAY.setRelease(array, offset, null);
                        ci++;
                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }
                    } else {
                        break;
                    }
                }

                emitted = e;
                consumed = c;
                consumerIndex = ci;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class MapWhenConditionalSubscriber<T, U, R> extends AbstractMapWhen<T, U, R> {

        final ConditionalSubscriber<? super R> actual;

        MapWhenConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch, boolean delayError) {
            super(mapper, combiner, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            ConditionalSubscriber<? super R> a = actual;
            Object[] arr = array;
            int m = arr.length - 1;
            int missed = 1;
            long e = emitted;
            int c = consumed;
            long ci = consumerIndex;
            int lim = prefetch - (prefetch >> 2);
            CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper = this.mapper;
            CheckedBiFunction<? super T, ? super U, ? extends R> combiner = this.combiner;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        result = null;
                        Arrays.fill(arr, null);
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        result = null;
                        Arrays.fill(arr, null);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    int offset = (int)ci & m;
                    T t = (T)ARRAY.getAcquire(arr, offset);
                    boolean empty = t == null;

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (e == r || empty) {
                        break;
                    }

                    int s = (int)STATE.getAcquire(this);
                    if (s == STATE_NONE) {

                        Flow.Publisher<? extends U> p;
                        try {
                            p = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null Flow.Publisher");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            result = null;
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        if (p instanceof FusedDynamicSource) {
                            FusedDynamicSource<U> fs = (FusedDynamicSource<U>) p;
                            STATE.setRelease(this, STATE_NONE);
                            ARRAY.setRelease(array, offset, null);
                            U u;

                            try {
                                u = fs.value();
                            } catch (Throwable ex) {
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayError) {
                                    upstream.cancel();
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    Arrays.fill(arr, null);
                                    a.onError(ex);
                                    return;
                                }
                                u = null;
                            }

                            if (u != null) {
                                R w;

                                try {
                                    w = Objects.requireNonNull(combiner.apply(t, u), "The combiner returned a null value");
                                } catch (Throwable ex) {
                                    ExceptionHelper.addThrowable(this, ERROR, ex);
                                    upstream.cancel();
                                    ex = ExceptionHelper.terminate(this, ERROR);
                                    result = null;
                                    Arrays.fill(arr, null);
                                    a.onError(ex);
                                    return;
                                }

                                if (a.tryOnNext(w)) {
                                    e++;
                                }
                            }

                            ci++;
                            if (++c == lim) {
                                c = 0;
                                upstream.request(lim);
                            }
                        } else {
                            MapWhenInnerSubscriber<U> inner = new MapWhenInnerSubscriber<>(this);
                            if (DisposableHelper.replace(this, OTHER, inner)) {
                                STATE.setRelease(this, STATE_RUNNING);
                                p.subscribe(inner);
                            } else {
                                return;
                            }
                        }
                    } else if (s == STATE_VALUE) {
                        STATE.setRelease(this, STATE_NONE);
                        ARRAY.setRelease(array, offset, null);
                        U u = result;
                        result = null;
                        R w;

                        try {
                            w = Objects.requireNonNull(combiner.apply(t, u), "The combiner returned a null value");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            result = null;
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        if (a.tryOnNext(w)) {
                            e++;
                        }

                        ci++;
                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }
                    } else if (s == STATE_EMPTY) {
                        STATE.setRelease(this, STATE_NONE);
                        ARRAY.setRelease(array, offset, null);
                        ci++;
                        if (++c == lim) {
                            c = 0;
                            upstream.request(lim);
                        }
                    } else {
                        break;
                    }
                }

                emitted = e;
                consumed = c;
                consumerIndex = ci;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class MapWhenInnerSubscriber<U> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<U>, AutoDisposable {

        final AbstractMapWhen<?, U, ?> parent;

        MapWhenInnerSubscriber(AbstractMapWhen<?, U, ?> parent) {
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
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                getPlain().cancel();
                setRelease(SubscriptionHelper.CANCELLED);

                parent.innerNext(item);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setRelease(SubscriptionHelper.CANCELLED);

                parent.innerError(throwable);
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setRelease(SubscriptionHelper.CANCELLED);

                parent.innerComplete();
            }
        }

        @Override
        public void close() {
            SubscriptionHelper.cancel(this);
        }
    }
}
