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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

public final class FolyamExpand<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander;

    final boolean depthFirst;

    final int capacityHint;

    public FolyamExpand(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander,
                   boolean depthFirst, int capacityHint) {
        this.source = source;
        this.expander = expander;
        this.depthFirst = depthFirst;
        this.capacityHint = capacityHint;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (depthFirst) {
            ExpandDepthSubscription<T> parent = new ExpandDepthSubscription<>(s, expander, capacityHint);
            parent.source = source;
            s.onSubscribe(parent);
        } else {
            ExpandBreathSubscriber<T> parent = new ExpandBreathSubscriber<>(s, expander, capacityHint);
            parent.queue.offer(source);
            s.onSubscribe(parent);
            parent.drainQueue();
        }
    }

    static final class ExpandBreathSubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        private static final long serialVersionUID = -8200116117441115256L;

        final FolyamSubscriber<? super T> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander;

        final PlainQueue<Flow.Publisher<? extends T>> queue;

        int wip;
        static final VarHandle WIP;

        volatile boolean active;

        long produced;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(ExpandBreathSubscriber.class, "wip", int.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        ExpandBreathSubscriber(FolyamSubscriber<? super T> actual,
                               CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander, int capacityHint) {
            this.actual = actual;
            this.expander = expander;
            this.queue = new SpscLinkedArrayQueue<>(capacityHint);
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            arbiterReplace(s);
        }

        @Override
        public void onNext(T t) {
            produced++;
            actual.onNext(t);

            Flow.Publisher<? extends T> p;
            try {
                p = Objects.requireNonNull(expander.apply(t), "The expander returned a null Publisher");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                super.cancel();
                actual.onError(ex);
                drainQueue();
                return;
            }

            queue.offer(p);
        }

        @Override
        public void onError(Throwable t) {
            arbiterReplace(SubscriptionHelper.CANCELLED);
            super.cancel();
            actual.onError(t);
            drainQueue();
        }

        @Override
        public void onComplete() {
            active = false;
            drainQueue();
        }

        @Override
        public void cancel() {
            super.cancel();
            drainQueue();
        }

        void drainQueue() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    PlainQueue<Flow.Publisher<? extends T>> q = queue;
                    if (arbiterIsCancelled()) {
                        q.clear();
                    } else {
                        if (!active) {
                            if (q.isEmpty()) {
                                arbiterReplace(SubscriptionHelper.CANCELLED);
                                super.cancel();
                                actual.onComplete();
                            } else {
                                Flow.Publisher<? extends T> p = q.poll();
                                long c = produced;
                                if (c != 0L) {
                                    produced = 0L;
                                    arbiterProduced(c);
                                }
                                active = true;
                                p.subscribe(this);
                            }
                        }
                    }
                } while ((int)WIP.getAndAdd(this, -1 ) -1 != 0);
            }
        }
    }

    static final class ExpandDepthSubscription<T>
            extends AtomicInteger
            implements Flow.Subscription {

        private static final long serialVersionUID = -2126738751597075165L;

        final FolyamSubscriber<? super T> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander;

        Throwable error;
        static final VarHandle ERROR;

        int active;
        static final VarHandle ACTIVE;

        long requested;
        static final VarHandle REQUESTED;

        Object current;
        static final VarHandle CURRENT;

        ArrayDeque<ExpandDepthSubscriber> subscriptionStack;

        volatile boolean cancelled;

        Flow.Publisher<? extends T> source;

        long consumed;

        static {
            try {
                ERROR = MethodHandles.lookup().findVarHandle(ExpandDepthSubscription.class, "error", Throwable.class);
                ACTIVE = MethodHandles.lookup().findVarHandle(ExpandDepthSubscription.class, "active", int.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(ExpandDepthSubscription.class, "requested", long.class);
                CURRENT = MethodHandles.lookup().findVarHandle(ExpandDepthSubscription.class, "current", Object.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        ExpandDepthSubscription(FolyamSubscriber<? super T> actual,
                                CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander,
                                int capacityHint) {
            this.actual = actual;
            this.expander = expander;
            this.subscriptionStack = new ArrayDeque<>();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drainQueue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                ArrayDeque<ExpandDepthSubscriber> q;
                synchronized (this) {
                    q = subscriptionStack;
                    subscriptionStack = null;
                }

                if (q != null) {
                    while (!q.isEmpty()) {
                        q.poll().dispose();
                    }
                }

                Object o = CURRENT.getAndSet(this, this);
                if (o != this && o != null) {
                    ((ExpandDepthSubscriber)o).dispose();
                }
            }
        }

        ExpandDepthSubscriber pop() {
            synchronized (this) {
                ArrayDeque<ExpandDepthSubscriber> q = subscriptionStack;
                return q != null ? q.pollFirst() : null;
            }
        }

        boolean push(ExpandDepthSubscriber subscriber) {
            synchronized (this) {
                ArrayDeque<ExpandDepthSubscriber> q = subscriptionStack;
                if (q != null) {
                    q.offerFirst(subscriber);
                    return true;
                }
                return false;
            }
        }

        boolean setCurrent(ExpandDepthSubscriber inner) {
            for (;;) {
                Object o = CURRENT.getAcquire(this);
                if (o == this) {
                    if (inner != null) {
                        inner.dispose();
                    }
                    return false;
                }
                if (CURRENT.compareAndSet(this, o, inner)) {
                    return true;
                }
            }
        }

        void drainQueue() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            long e = consumed;

            for (;;) {
                Object o = CURRENT.getAcquire(this);
                if (cancelled || o == this) {
                    source = null;
                    return;
                }

                @SuppressWarnings("unchecked")
                ExpandDepthSubscriber curr = (ExpandDepthSubscriber)o;
                Flow.Publisher<? extends T> p = source;

                if (curr == null && p != null) {
                    source = null;
                    ACTIVE.getAndAdd(this, 1);

                    ExpandDepthSubscriber eds = new ExpandDepthSubscriber();
                    curr = eds;
                    if (setCurrent(eds)) {
                        p.subscribe(eds);
                    } else {
                        return;
                    }
                } else {

                    boolean currentDone = curr.done;
                    T v = curr.value;

                    boolean newSource = false;
                    if (v != null && e != (long)REQUESTED.getAcquire(this)) {
                        curr.value = null;
                        a.onNext(v);
                        e++;

                        try {
                            p = Objects.requireNonNull(expander.apply(v), "The expander returned a null Publisher");
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            p = null;
                            curr.dispose();
                            curr.done = true;
                            currentDone = true;
                            v = null;
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                        }

                        if (p != null) {
                            if (push(curr)) {
                                ACTIVE.getAndAdd(this, 1);
                                curr = new ExpandDepthSubscriber();
                                if (setCurrent(curr)) {
                                    p.subscribe(curr);
                                    newSource = true;
                                } else {
                                    return;
                                }
                            }
                        }
                    }

                    if (!newSource) {
                        if (currentDone && v == null) {
                            if ((int)ACTIVE.getAndAdd(this, -1) - 1 == 0) {
                                Throwable ex = ExceptionHelper.terminate(this, ERROR);
                                if (ex != null) {
                                    a.onError(ex);
                                } else {
                                    a.onComplete();
                                }
                                return;
                            }
                            curr = pop();
                            if (curr != null && setCurrent(curr)) {
                                curr.requestOne();
                                continue;
                            } else {
                                return;
                            }
                        }
                    }
                }
                consumed = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        void innerNext(ExpandDepthSubscriber inner, T t) {
            drainQueue();
        }

        void innerError(ExpandDepthSubscriber inner, Throwable t) {
            ExceptionHelper.addThrowable(this, ERROR, t);
            inner.done = true;
            drainQueue();
        }

        void innerComplete(ExpandDepthSubscriber inner) {
            inner.done = true;
            drainQueue();
        }

        final class ExpandDepthSubscriber
                extends AtomicReference<Flow.Subscription>
                implements FolyamSubscriber<T> {

            private static final long serialVersionUID = 4198645419772153739L;

            volatile boolean done;

            volatile T value;

            @Override
            public void onSubscribe(Flow.Subscription s) {
                if (SubscriptionHelper.replace(this, s)) {
                    s.request(1);
                }
            }

            @Override
            public void onNext(T t) {
                if (getAcquire() != SubscriptionHelper.CANCELLED) {
                    value = t;
                    innerNext(this, t);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (getAcquire() != SubscriptionHelper.CANCELLED) {
                    innerError(this, t);
                }
            }

            @Override
            public void onComplete() {
                if (getAcquire() != SubscriptionHelper.CANCELLED) {
                    innerComplete(this);
                }
            }

            public void requestOne() {
                get().request(1);
            }

            public void dispose() {
                SubscriptionHelper.cancel(this);
            }
        }
    }
}
