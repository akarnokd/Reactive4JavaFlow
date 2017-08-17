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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class FolyamBufferStartEnd<T, U, C extends Collection<? super T>> extends Folyam<C> {

    final Folyam<T> source;

    final Flow.Publisher<U> start;

    final CheckedFunction<? super U, ? extends Flow.Publisher<?>> end;

    final Callable<C> collectionSupplier;

    public FolyamBufferStartEnd(Folyam<T> source, Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end, Callable<C> collectionSupplier) {
        this.source = source;
        this.start = start;
        this.end = end;
        this.collectionSupplier = collectionSupplier;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super C> s) {
        BufferStartEndSubscriber<T, U, C> parent = new BufferStartEndSubscriber<>(s, end, collectionSupplier);
        s.onSubscribe(parent);
        start.subscribe(parent.openSubscriber);
        source.subscribe(parent);
    }

    interface BufferStartSupport<U> {

        void open(long index, U key);

        void startError(Throwable throwable);

        void startComplete();
    }

    interface BufferEndSupport<U> {
        void close(long index, BufferEndSubscriber<U> sender);

        void closeError(Throwable throwable, BufferEndSubscriber<U> sender);
    }

    static final class BufferStartEndSubscriber<T, U, C extends Collection<? super T>> extends AtomicInteger implements FolyamSubscriber<T>, Flow.Subscription, BufferStartSupport<U>, BufferEndSupport<U> {

        final FolyamSubscriber<? super C> actual;

        final CheckedFunction<? super U, ? extends Flow.Publisher<?>> end;

        final Callable<C> collectionSupplier;

        final SpscLinkedArrayQueue<Object> queue;

        final CompositeAutoDisposable subscribers;

        final Map<Long, C> buffers;

        final BufferStartSubscriber<U> openSubscriber;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), BufferStartEndSubscriber.class, "upstream", Flow.Subscription.class);

        volatile boolean cancelled;

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), BufferStartEndSubscriber.class, "error", Throwable.class);

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), BufferStartEndSubscriber.class, "done", boolean.class);

        static final Object ITEM = new Object();

        static final Object OPEN = new Object();

        static final Object CLOSE = new Object();

        BufferStartEndSubscriber(FolyamSubscriber<? super C> actual, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end, Callable<C> collectionSupplier) {
            this.actual = actual;
            this.end = end;
            this.collectionSupplier = collectionSupplier;
            this.queue = new SpscLinkedArrayQueue<>(FolyamPlugins.defaultBufferSize());
            this.subscribers = new CompositeAutoDisposable();
            this.buffers = new LinkedHashMap<>();
            this.openSubscriber = new BufferStartSubscriber<>(this);
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
            cancelAll();
        }

        void cancelAll() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            openSubscriber.close();
            subscribers.close();
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }
            int missed = 1;

            FolyamSubscriber<? super C> a = this.actual;
            SpscLinkedArrayQueue<Object> q = this.queue;
            Map<Long, C> buffers = this.buffers;

            for (;;) {

                for (;;) {
                    if (cancelled) {
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

                    if (d && empty && buffers.isEmpty()) {
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
                        for (C b : buffers.values()) {
                            b.add(t);
                        }
                    } else if (action == OPEN) {
                        BufferEndSubscriber<U> s = (BufferEndSubscriber<U>)parameter;
                        Flow.Publisher<?> p = s.source;

                        try {
                            buffers.put(s.index, collectionSupplier.call());
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            cancelAll();
                            continue;
                        }
                        s.source = null;
                        p.subscribe(s);
                    } else {
                        Long index = (Long)parameter;
                        C b = buffers.remove(index);
                        a.onNext(b);
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
            BufferEndSubscriber<U> s = new BufferEndSubscriber<>(this, index, p);
            if (subscribers.add(s)) {
                submit(OPEN, s);
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
        public void close(long index, BufferEndSubscriber<U> sender) {
            subscribers.delete(sender);
            submit(CLOSE, index);
        }

        @Override
        public void closeError(Throwable ex, BufferEndSubscriber<U> sender) {
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

    static final class BufferStartSubscriber<U> implements FolyamSubscriber<U> {

        final BufferStartSupport<U> parent;

        long index;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), BufferStartSubscriber.class, "upstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), BufferStartSubscriber.class, "requested", long.class);

        BufferStartSubscriber(BufferStartSupport<U> parent) {
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public void onNext(U item) {
            parent.open(index++, item);
        }

        @Override
        public void onError(Throwable throwable) {
            parent.startError(throwable);
        }

        @Override
        public void onComplete() {
            parent.startComplete();
        }

        void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        void close() {
            SubscriptionHelper.cancel(this, UPSTREAM);
        }
    }

    static final class BufferEndSubscriber<U> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

        final BufferEndSupport<U> parent;

        final long index;

        Flow.Publisher<?> source;

        BufferEndSubscriber(BufferEndSupport<U> parent, long index, Flow.Publisher<?> source) {
            this.parent = parent;
            this.index = index;
            this.source = source;
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
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                getPlain().cancel();
                setPlain(SubscriptionHelper.CANCELLED);
                parent.close(index, this);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.closeError(throwable, this);
            }
        }

        @Override
        public void onComplete() {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.close(index, this);
            }
        }
    }
}
