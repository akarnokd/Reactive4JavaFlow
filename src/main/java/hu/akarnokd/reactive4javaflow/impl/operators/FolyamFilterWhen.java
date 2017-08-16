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

public final class FolyamFilterWhen<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter;

    final int prefetch;

    final boolean delayError;

    public FolyamFilterWhen(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch, boolean delayError) {
        this.source = source;
        this.filter = filter;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FilterWhenConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, filter, prefetch, delayError));
        } else {
            source.subscribe(new FilterWhenSubscriber<>(s, filter, prefetch, delayError));
        }
    }

    static abstract class AbstractFilterWhen<T> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription {

        final CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter;

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
                PRODUCER_INDEX = lk.findVarHandle(AbstractFilterWhen.class, "producerIndex", long.class);
                CONSUMER_INDEX = lk.findVarHandle(AbstractFilterWhen.class, "consumerIndex", long.class);
                DONE = lk.findVarHandle(AbstractFilterWhen.class, "done", boolean.class);
                STATE = lk.findVarHandle(AbstractFilterWhen.class, "state", int.class);
                ERROR = lk.findVarHandle(AbstractFilterWhen.class, "error", Throwable.class);
                REQUESTED = lk.findVarHandle(AbstractFilterWhen.class, "requested", long.class);
                OTHER = lk.findVarHandle(AbstractFilterWhen.class, "other", AutoDisposable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        }


        AbstractFilterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch, boolean delayError) {
            this.filter = filter;
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
                    DisposableHelper.close(this, OTHER);
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
            DisposableHelper.close(this, OTHER);
            if (getAndIncrement() == 0) {
                Arrays.fill(array, null);
            }
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        final void innerNext() {
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

    static final class FilterWhenSubscriber<T> extends AbstractFilterWhen<T> {

        final FolyamSubscriber<? super T> actual;

        FilterWhenSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch, boolean delayError) {
            super(filter, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            FolyamSubscriber<? super T> a = actual;
            Object[] arr = array;
            int m = arr.length - 1;
            int missed = 1;
            long e = emitted;
            int c = consumed;
            long ci = consumerIndex;
            int lim = prefetch - (prefetch >> 2);
            CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter = this.filter;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        Arrays.fill(arr, null);
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
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

                        Flow.Publisher<Boolean> p;
                        try {
                            p = Objects.requireNonNull(filter.apply(t), "The filter returned a null Flow.Publisher");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        if (p instanceof FusedDynamicSource) {
                            FusedDynamicSource<Boolean> fs = (FusedDynamicSource<Boolean>) p;
                            STATE.setRelease(this, STATE_NONE);
                            ARRAY.setRelease(array, offset, null);
                            Boolean u;

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

                            if (u != null && u) {
                                a.onNext(t);
                                e++;
                            }

                            ci++;
                            if (++c == lim) {
                                c = 0;
                                upstream.request(lim);
                            }
                        } else {
                            MapWhenInnerSubscriber inner = new MapWhenInnerSubscriber(this);
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

                        a.onNext(t);

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

    static final class FilterWhenConditionalSubscriber<T> extends AbstractFilterWhen<T> {

        final ConditionalSubscriber<? super T> actual;

        FilterWhenConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch, boolean delayError) {
            super(filter, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            ConditionalSubscriber<? super T> a = actual;
            Object[] arr = array;
            int m = arr.length - 1;
            int missed = 1;
            long e = emitted;
            int c = consumed;
            long ci = consumerIndex;
            int lim = prefetch - (prefetch >> 2);
            CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter = this.filter;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                for (;;) {
                    if (cancelled) {
                        Arrays.fill(arr, null);
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
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

                        Flow.Publisher<Boolean> p;
                        try {
                            p = Objects.requireNonNull(filter.apply(t), "The filter returned a null Flow.Publisher");
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            upstream.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            Arrays.fill(arr, null);
                            a.onError(ex);
                            return;
                        }

                        if (p instanceof FusedDynamicSource) {
                            FusedDynamicSource<Boolean> fs = (FusedDynamicSource<Boolean>) p;
                            STATE.setRelease(this, STATE_NONE);
                            ARRAY.setRelease(array, offset, null);
                            Boolean u;

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

                            if (u != null && u && a.tryOnNext(t)) {
                                e++;
                            }

                            ci++;
                            if (++c == lim) {
                                c = 0;
                                upstream.request(lim);
                            }
                        } else {
                            MapWhenInnerSubscriber inner = new MapWhenInnerSubscriber(this);
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

                        if (a.tryOnNext(t)) {
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

    static final class MapWhenInnerSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Boolean>, AutoDisposable {

        final AbstractFilterWhen<?> parent;

        MapWhenInnerSubscriber(AbstractFilterWhen<?> parent) {
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, subscription)) {
               subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(Boolean item) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                getPlain().cancel();
                setRelease(SubscriptionHelper.CANCELLED);

                if (item) {
                    parent.innerNext();
                } else {
                    parent.innerComplete();
                }
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
