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
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamSwitchMap<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int prefetch;

    final boolean delayError;

    public FolyamSwitchMap(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
        this.source = source;
        this.mapper = mapper;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new SwitchMapConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, prefetch, delayError));
        } else {
            source.subscribe(new SwitchMapSubscriber<>(s, mapper, prefetch, delayError));
        }
    }

    static abstract class AbstractSwitchMap<T, R> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, QueuedFolyamSubscriberSupport<R> {

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        final int prefetch;

        final boolean delayError;

        Flow.Subscription upstream;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractSwitchMap.class, "requested", long.class);

        QueuedInnerFolyamSubscriber<T> current;
        static final VarHandle CURRENT = VH.find(MethodHandles.lookup(), AbstractSwitchMap.class, "current", QueuedInnerFolyamSubscriber.class);

        volatile boolean cancelled;

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractSwitchMap.class, "done", boolean.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractSwitchMap.class, "error", Throwable.class);

        long emitted;

        static final QueuedInnerFolyamSubscriber TERMINATED;

        static {
            TERMINATED = new QueuedInnerFolyamSubscriber<Object>(null, Integer.MAX_VALUE, 0);
            TERMINATED.cancel();
        }

        protected AbstractSwitchMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
            this.mapper = mapper;
            this.prefetch = prefetch;
            this.delayError = delayError;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            onStart();
            upstream.request(Long.MAX_VALUE);
        }

        abstract void onStart();

        @Override
        public final void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }

        abstract void drainLoop();

        @Override
        public final void onNext(T item) {
            Flow.Publisher<? extends R> p;

            try {
                p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Flow.Publisher");
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return;
            }

            QueuedInnerFolyamSubscriber<R> inner = new QueuedInnerFolyamSubscriber<>(this, 0, prefetch);
            for (;;) {
                QueuedInnerFolyamSubscriber<R> a = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAcquire(this);
                if (a == TERMINATED) {
                    break;
                }
                if (CURRENT.compareAndSet(this, a, inner)) {
                    if (a != null) {
                        a.cancel();
                    }
                    p.subscribe(inner);
                    break;
                }
            }
        }

        @Override
        public final void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                if (!delayError) {
                    upstream.cancel();
                    cancelCurrent();
                }
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
            cancelCurrent();
        }

        final void cancelCurrent() {
            QueuedInnerFolyamSubscriber<R> a = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAndSet(this, TERMINATED);
            if (a != null && a != TERMINATED) {
                a.cancel();
            }
        }

        @Override
        public final void innerError(QueuedInnerFolyamSubscriber<R> sender, int index, Throwable ex) {
            if (CURRENT.getAcquire(this) == sender && ExceptionHelper.addThrowable(this, ERROR, ex)) {
                sender.setDone();
                if (!delayError) {
                    upstream.cancel();
                    cancelCurrent();
                    DONE.setRelease(this, true);
                }
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }
    }

    static final class SwitchMapSubscriber<T, R> extends AbstractSwitchMap<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected SwitchMapSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
            super(mapper, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            FolyamSubscriber<? super R> a = actual;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R> c = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAcquire(this);

                    if (c == TERMINATED) {
                        return;
                    }
                    boolean empty = c == null;
                    if (d && empty) {
                        CURRENT.setRelease(this, TERMINATED);
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                    if (empty) {
                        break;
                    }

                    d = c.isDone();
                    FusedQueue<R> q = c.getQueue();

                    R v;

                    try {
                        v = q != null ? q.poll() : null;
                    } catch (Throwable ex) {
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        if (!delayError) {
                            cancelCurrent();
                            a.onError(ExceptionHelper.terminate(this, ERROR));
                            return;
                        } else {
                            c.cancel();
                            CURRENT.compareAndSet(this, c, null);
                        }
                        v = null;
                        d = true;
                    }

                    empty = v == null;

                    if (d && empty) {
                        CURRENT.compareAndSet(this, c, null);
                        continue;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;

                    c.request();
                }

                if (e == r) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R> c = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAcquire(this);

                    if (c == TERMINATED) {
                        return;
                    }
                    boolean empty = c == null;
                    if (d && empty) {
                        CURRENT.setRelease(this, TERMINATED);
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                    if (!empty) {
                        d = c.isDone();
                        FusedQueue<R> q = c.getQueue();
                        empty = q == null || q.isEmpty();

                        if (d && empty) {
                            CURRENT.compareAndSet(this, c, null);
                            continue;
                        }
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class SwitchMapConditionalSubscriber<T, R> extends AbstractSwitchMap<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected SwitchMapConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
            super(mapper, prefetch, delayError);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainLoop() {
            int missed = 1;
            ConditionalSubscriber<? super R> a = actual;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R> c = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAcquire(this);

                    if (c == TERMINATED) {
                        return;
                    }
                    boolean empty = c == null;
                    if (d && empty) {
                        CURRENT.setRelease(this, TERMINATED);
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                    if (empty) {
                        break;
                    }

                    d = c.isDone();
                    FusedQueue<R> q = c.getQueue();

                    R v;

                    try {
                        v = q != null ? q.poll() : null;
                    } catch (Throwable ex) {
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        if (!delayError) {
                            cancelCurrent();
                            a.onError(ExceptionHelper.terminate(this, ERROR));
                            return;
                        } else {
                            c.cancel();
                            CURRENT.compareAndSet(this, c, null);
                        }
                        v = null;
                        d = true;
                    }

                    empty = v == null;

                    if (d && empty) {
                        CURRENT.compareAndSet(this, c, null);
                        continue;
                    }

                    if (empty) {
                        break;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    c.request();
                }

                if (e == r) {
                    if (cancelled) {
                        return;
                    }

                    if (!delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    QueuedInnerFolyamSubscriber<R> c = (QueuedInnerFolyamSubscriber<R>)CURRENT.getAcquire(this);

                    if (c == TERMINATED) {
                        return;
                    }
                    boolean empty = c == null;
                    if (d && empty) {
                        CURRENT.setRelease(this, TERMINATED);
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                    if (!empty) {
                        d = c.isDone();
                        FusedQueue<R> q = c.getQueue();
                        empty = q == null || q.isEmpty();

                        if (d && empty) {
                            CURRENT.compareAndSet(this, c, null);
                            continue;
                        }
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
