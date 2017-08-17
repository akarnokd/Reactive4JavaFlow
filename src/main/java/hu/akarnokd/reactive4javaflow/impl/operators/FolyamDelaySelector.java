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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

public final class FolyamDelaySelector<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector;

    public FolyamDelaySelector(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
        this.source = source;
        this.delaySelector = delaySelector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new DelaySelectorSubscriber<>(s, delaySelector));
    }

    static final class DelaySelectorSubscriber<T> extends AtomicLong implements FolyamSubscriber<T>, FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector;

        final PlainQueue<T> queue;

        Flow.Subscription upstream;

        volatile boolean cancelled;

        long active;
        static final VarHandle ACTIVE = VH.find(MethodHandles.lookup(), DelaySelectorSubscriber.class, "active", long.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), DelaySelectorSubscriber.class, "error", Throwable.class);

        OpenHashSet<AutoDisposable> subscribers;

        boolean outputFused;

        DelaySelectorSubscriber(FolyamSubscriber<? super T> actual, CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
            this.actual = actual;
            this.delaySelector = delaySelector;
            this.queue = new MpscLinkedArrayQueue<>(32);
            this.subscribers = new OpenHashSet<>();
            ACTIVE.setRelease(this, 1);
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public final void onNext(T item) {

            Flow.Publisher<?> p;

            try {
                p = Objects.requireNonNull(delaySelector.apply(item), "The delaySelector returned a null Flow.Publisher");
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return;
            }

            DelaySelectorInnerSubscriber<T> inner = new DelaySelectorInnerSubscriber<>(this, item);
            if (add(inner)) {
                ACTIVE.getAndAdd(this, 1);
                p.subscribe(inner);
            }
        }

        @Override
        public final void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                ACTIVE.getAndAdd(this, -1);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public final void onComplete() {
            ACTIVE.getAndAdd(this, -1);
            drain();
        }

        void drain() {
            if (getAndIncrement() == 0) {
                if (outputFused) {
                    drainFused();
                } else {
                    drainLoop();
                }
            }
        }

        void drainFused() {
            long missed = 1L;
            PlainQueue<T> q = queue;
            FolyamSubscriber<? super T> a = actual;

            for (;;) {
                if (cancelled) {
                    q.clear();
                    return;
                }

                boolean d = (long) ACTIVE.getAcquire(this) == 0L;

                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    if (ex != null) {
                        a.onError(ex);
                    } else {
                        a.onComplete();
                    }
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0L) {
                    break;
                }
            }
        }

        void drainLoop() {
            long missed = 1L;
            PlainQueue<T> q = queue;
            FolyamSubscriber<? super T> a = actual;

            for (;;) {

                for (;;) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = (long) ACTIVE.getAcquire(this) == 0L;

                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);
                }

                missed = addAndGet(-missed);
                if (missed == 0L) {
                    break;
                }
            }
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
            cancelSubscribers();
            if (getAndIncrement() == 0) {
                queue.clear();
            }
        }
        boolean add(AutoDisposable d) {
            if (!cancelled) {
                synchronized (this) {
                    if (!cancelled) {
                        OpenHashSet<AutoDisposable> subs = this.subscribers;
                        if (subs != null) {
                            subs.add(d);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        void remove(AutoDisposable d) {
            if (!cancelled) {
                synchronized (this) {
                    if (!cancelled) {
                        OpenHashSet<AutoDisposable> subs = this.subscribers;
                        if (subs != null) {
                            subs.remove(d);
                        }
                    }
                }
            }
        }

        void cancelSubscribers() {
            OpenHashSet<AutoDisposable> set;
            synchronized (this) {
                set = subscribers;
                subscribers = null;
            }
            if (set != null) {
                Object[] o = set.keys();
                for (Object e : o) {
                    if (e != null) {
                        ((AutoDisposable) e).close();
                    }
                }
            }
        }

        void innerNext(T item) {
            queue.offer(item);
            ACTIVE.getAndAdd(this, -1);
            drain();
        }

        void innerError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                ACTIVE.getAndAdd(this, -1);
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public T poll() throws Throwable {
            return queue.poll();
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            queue.clear();
        }
    }

    static final class DelaySelectorInnerSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

        final DelaySelectorSubscriber<T> parent;

        final T item;

        DelaySelectorInnerSubscriber(DelaySelectorSubscriber<T> parent, T item) {
            this.parent = parent;
            this.item = item;
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
                parent.innerNext(this.item);
                parent.remove(this);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerError(throwable);
                parent.remove(this);
            }
        }

        @Override
        public void onComplete() {
            if (getPlain() != SubscriptionHelper.CANCELLED) {
                setPlain(SubscriptionHelper.CANCELLED);
                parent.innerNext(this.item);
                parent.remove(this);
            }
        }
    }

}
