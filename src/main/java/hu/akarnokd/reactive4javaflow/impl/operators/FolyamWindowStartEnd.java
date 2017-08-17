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
import hu.akarnokd.reactive4javaflow.disposables.CompositeAutoDisposable;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;
import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamWindowStartEnd<T, U> extends Folyam<Folyam<T>> {

    final Folyam<T> source;

    final Flow.Publisher<U> start;

    final CheckedFunction<? super U, ? extends Flow.Publisher<?>> end;

    public FolyamWindowStartEnd(Folyam<T> source, Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end) {
        this.source = source;
        this.start = start;
        this.end = end;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Folyam<T>> s) {
        BufferStartEndSubscriber<T, U> parent = new BufferStartEndSubscriber<>(s, end);
        s.onSubscribe(parent);
        start.subscribe(parent.openSubscriber);
        source.subscribe(parent);
    }

    static final class BufferStartEndSubscriber<T, U> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, FolyamBufferStartEnd.BufferStartSupport<U>, FolyamBufferStartEnd.BufferEndSupport<U>, Runnable {

        final FolyamSubscriber<? super Folyam<T>> actual;

        final CheckedFunction<? super U, ? extends Flow.Publisher<?>> end;

        final SpscLinkedArrayQueue<Object> queue;

        final CompositeAutoDisposable subscribers;

        final Map<Long, SolocastProcessor<T>> buffers;

        final FolyamBufferStartEnd.BufferStartSubscriber<U> openSubscriber;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        volatile boolean cancelled;

        Throwable error;
        static final VarHandle ERROR;

        boolean done;
        static final VarHandle DONE;

        static final Object ITEM = new Object();

        static final Object OPEN = new Object();

        static final Object CLOSE = new Object();

        boolean once;
        static final VarHandle ONCE;

        int active;
        static final VarHandle ACTIVE;

        static {
            try {
                DONE = MethodHandles.lookup().findVarHandle(BufferStartEndSubscriber.class, "done", boolean.class);
                UPSTREAM = MethodHandles.lookup().findVarHandle(BufferStartEndSubscriber.class, "upstream", Flow.Subscription.class);
                ERROR = MethodHandles.lookup().findVarHandle(BufferStartEndSubscriber.class, "error", Throwable.class);
                ONCE = MethodHandles.lookup().findVarHandle(BufferStartEndSubscriber.class, "once", boolean.class);
                ACTIVE = MethodHandles.lookup().findVarHandle(BufferStartEndSubscriber.class, "active", int.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        BufferStartEndSubscriber(FolyamSubscriber<? super Folyam<T>> actual, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end) {
            this.actual = actual;
            this.end = end;
            this.queue = new SpscLinkedArrayQueue<>(FolyamPlugins.defaultBufferSize());
            this.subscribers = new CompositeAutoDisposable();
            this.buffers = new LinkedHashMap<>();
            this.openSubscriber = new FolyamBufferStartEnd.BufferStartSubscriber<>(this);
            ACTIVE.setRelease(this, 1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
                subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T item) {
            submit(ITEM, item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                openSubscriber.close();
                subscribers.close();
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            openSubscriber.close();
            subscribers.close();
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            openSubscriber.request(n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            openSubscriber.close();
            if (ONCE.compareAndSet(this, false, true)) {
                if ((int)ACTIVE.getAndAdd(this, -1) -1 == 0) {
                    SubscriptionHelper.cancel(this, UPSTREAM);
                }
            }
        }

        void cancelAll() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            openSubscriber.close();
            subscribers.close();
        }

        @Override
        public void run() {
            if ((int)ACTIVE.getAndAdd(this, -1) -1 == 0) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                openSubscriber.close();
            }
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }
            int missed = 1;

            FolyamSubscriber<? super Folyam<T>> a = this.actual;
            SpscLinkedArrayQueue<Object> q = this.queue;
            Map<Long, SolocastProcessor<T>> buffers = this.buffers;

            for (;;) {

                for (;;) {
                    if ((int)ACTIVE.getAcquire(this) == 0) {
                        q.clear();
                        buffers.clear();
                        return;
                    }

                    if (ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        q.clear();
                        buffers.clear();
                        a.onError(ex);
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = q.isEmpty();

                    if (d && empty) {
                        for (SolocastProcessor<T> sp : buffers.values()) {
                            sp.onComplete();
                        }
                        buffers.clear();
                        a.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    Object action = q.poll();
                    Object parameter = q.poll();

                    if (action == ITEM) {
                        T t = (T)parameter;
                        for (SolocastProcessor<T> b : buffers.values()) {
                            b.onNext(t);
                        }
                    } else if (action == OPEN) {
                        if (!cancelled) {
                            FolyamBufferStartEnd.BufferEndSubscriber<U> s = (FolyamBufferStartEnd.BufferEndSubscriber<U>) parameter;
                            Flow.Publisher<?> p = s.source;
                            s.source = null;

                            ACTIVE.getAndAdd(this, 1);

                            SolocastProcessor<T> sp = new SolocastProcessor<>(FolyamPlugins.defaultBufferSize(), this);
                            buffers.put(s.index, sp);

                            a.onNext(sp);

                            p.subscribe(s);
                        }
                    } else {
                        Long index = (Long)parameter;
                        SolocastProcessor<T> b = buffers.remove(index);
                        b.onComplete();
                    }
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public void open(long index, U key) {
            if (!cancelled) {
                Flow.Publisher<?> p;
                try {
                    p = Objects.requireNonNull(end.apply(key), "The end function returned a null Flow.Publisher");
                } catch (Throwable ex) {
                    if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                        SubscriptionHelper.cancel(this, UPSTREAM);
                        openSubscriber.close();
                        subscribers.close();
                        DONE.setRelease(this, true);
                        drain();
                    } else {
                        FolyamPlugins.onError(ex);
                    }
                    return;
                }
                FolyamBufferStartEnd.BufferEndSubscriber<U> s = new FolyamBufferStartEnd.BufferEndSubscriber<>(this, index, p);
                if (subscribers.add(s)) {
                    submit(OPEN, s);
                }
            }
        }

        @Override
        public void startError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                subscribers.close();
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public void startComplete() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void close(long index, FolyamBufferStartEnd.BufferEndSubscriber<U> sender) {
            subscribers.delete(sender);
            submit(CLOSE, index);
        }

        @Override
        public void closeError(Throwable ex, FolyamBufferStartEnd.BufferEndSubscriber<U> sender) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                cancelAll();
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        void submit(Object action, Object parameter) {
            synchronized (this) {
                queue.offer(action, parameter);
            }
            drain();
        }
    }
}
