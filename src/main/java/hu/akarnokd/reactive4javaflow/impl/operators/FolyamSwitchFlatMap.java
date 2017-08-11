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

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.impl.ExceptionHelper;
import hu.akarnokd.reactive4javaflow.impl.PlainQueue;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class FolyamSwitchFlatMap<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int maxActive;

    final int bufferSize;

    public FolyamSwitchFlatMap(Folyam<T> source,
                        CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper,
                          int maxActive, int bufferSize) {
        super();
        this.source = source;
        this.mapper = mapper;
        this.maxActive = maxActive;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        source.subscribe(new SwitchFlatMapSubscriber<T, R>(s, mapper, maxActive, bufferSize));
    }

    static final class SwitchFlatMapSubscriber<T, R>
            extends AtomicInteger
            implements FolyamSubscriber<T>, Flow.Subscription {

        private static final long serialVersionUID = 6801374887555723721L;

        final FolyamSubscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        final int maxActive;

        final int bufferSize;

        final ArrayDeque<SfmInnerSubscriber<T, R>> active;

        long requested;
        static final VarHandle REQUESTED;

        Throwable error;
        static final VarHandle ERROR;

        Flow.Subscription s;

        volatile boolean done;

        volatile boolean cancelled;

        volatile long version;

        final SfmInnerSubscriber<T, R>[] activeCache;
        long versionCache;

        long emitted;

        static {
            try {
                REQUESTED = MethodHandles.lookup().findVarHandle(SwitchFlatMapSubscriber.class, "requested", long.class);
                ERROR = MethodHandles.lookup().findVarHandle(SwitchFlatMapSubscriber.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        @SuppressWarnings("unchecked")
        SwitchFlatMapSubscriber(FolyamSubscriber<? super R> actual,
                                CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxActive,
                                int bufferSize) {
            this.actual = actual;
            this.mapper = mapper;
            this.maxActive = maxActive;
            this.bufferSize = bufferSize;
            this.active = new ArrayDeque<>();
            this.activeCache = new SfmInnerSubscriber[maxActive];
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);

            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            Flow.Publisher<? extends R> p;
            try {
                p = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null Publisher");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                s.cancel();
                onError(ex);
                return;
            }

            SfmInnerSubscriber<T, R> inner = new SfmInnerSubscriber<>(this, bufferSize);
            if (add(inner)) {
                p.subscribe(inner);
            }
        }

        boolean add(SfmInnerSubscriber<T, R> inner) {
            SfmInnerSubscriber<T, R> evicted = null;
            synchronized (this) {
                if (cancelled) {
                    return false;
                }

                if (active.size() == maxActive) {
                    evicted = active.poll();
                }
                active.offer(inner);
                version++;
            }

            if (evicted != null) {
                evicted.cancel();
            }
            return true;
        }

        void remove(SfmInnerSubscriber<T, R> inner) {
            synchronized (this) {
                active.remove(inner);
                version++;
            }
        }

        @Override
        public void onError(Throwable t) {
            if (ERROR.compareAndSet(this, null, t)) {
                cancelInners();
                done = true;
                drain();
            } else {
                FolyamPlugins.onError(t);
            }
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                s.cancel();
                cancelInners();
                if (getAndIncrement() == 0) {
                    clearCache();
                }
            }
        }

        void clearCache() {
            Arrays.fill(activeCache, null);
        }

        void cancelInners() {
            List<SfmInnerSubscriber<T, R>> subscribers = new ArrayList<>();
            synchronized (this) {
                subscribers.addAll(active);
                active.clear();
            }
            for (SfmInnerSubscriber<T, R> inner : subscribers) {
                inner.cancel();
            }
        }

        void innerError(Throwable t) {
            if (ERROR.compareAndSet(this, null, t)) {
                s.cancel();
                cancelInners();
                done = true;
                drain();
            } else {
                FolyamPlugins.onError(t);
            }
        }

        void updateInners() {
            SfmInnerSubscriber<T, R>[] a = activeCache;
            if (versionCache != version) {
                synchronized (this) {
                    int i = 0;
                    Iterator<SfmInnerSubscriber<T, R>> it = active.iterator();
                    while (it.hasNext()) {
                        a[i++] = it.next();
                    }
                    for (int j = i; j < a.length; j++) {
                        a[j] = null;
                    }
                    versionCache = version;
                }
            }
        }

        void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                FolyamSubscriber<? super R> a = actual;
                SfmInnerSubscriber<T, R>[] inners = activeCache;
                long e = emitted;

                outer:
                for (;;) {
                    long r = (long)REQUESTED.getAcquire(this);


                    for (;;) {
                        if (cancelled) {
                            clearCache();
                            return;
                        }

                        boolean d = done;

                        updateInners();
                        long ver = versionCache;


                        if (d) {
                            Throwable ex = (Throwable)ERROR.getAcquire(this);
                            if (ex != null) {
                                clearCache();

                                a.onError(ExceptionHelper.terminate(this, ERROR));
                                return;
                            } else
                            if (inners[0] == null) {
                                a.onComplete();
                                return;
                            }
                        }

                        int becameEmpty = 0;
                        int activeCount = 0;

                        draining:
                        for (SfmInnerSubscriber<T, R> inner : inners) {
                            if (cancelled) {
                                clearCache();
                                return;
                            }

                            if (inner == null) {
                                break;
                            }
                            if (ver != version) {
                                continue outer;
                            }

                            activeCount++;

                            long f = 0;

                            PlainQueue<R> q = inner.queue;

                            while (e != r) {
                                if (cancelled) {
                                    clearCache();
                                    return;
                                }

                                Throwable ex = (Throwable)ERROR.getAcquire(this);
                                if (ex != null) {
                                    clearCache();

                                    a.onError(ExceptionHelper.terminate(this, ERROR));
                                    return;
                                }

                                if (ver != version) {
                                    if (f != 0L) {
                                        inner.produced(f);
                                    }
                                    continue outer;
                                }

                                boolean d2 = inner.done;
                                R v = q.poll();
                                boolean empty = v == null;

                                if (d2 && empty) {
                                    remove(inner);
                                    continue draining;
                                }

                                if (empty) {
                                    if (f != 0L) {
                                        inner.produced(f);
                                        f = 0L;
                                    }
                                    becameEmpty++;
                                    break;
                                }

                                a.onNext(v);
                                e++;
                                f++;
                            }

                            if (inner.done && q.isEmpty()) {
                                remove(inner);
                            } else
                            if (f != 0L) {
                                inner.produced(f);
                            }
                        }

                        if (becameEmpty == activeCount || e == r) {
                            break;
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

        static final class SfmInnerSubscriber<T, R> extends AtomicReference<Flow.Subscription>
                implements FolyamSubscriber<R> {

            private static final long serialVersionUID = 4011255448052082638L;

            final SwitchFlatMapSubscriber<T, R> parent;

            final int bufferSize;

            final int limit;

            final PlainQueue<R> queue;

            long produced;

            volatile boolean done;

            SfmInnerSubscriber(SwitchFlatMapSubscriber<T, R> parent, int bufferSize) {
                this.parent = parent;
                this.bufferSize = bufferSize;
                this.limit = bufferSize - (bufferSize >> 2);
                this.queue = new SpscArrayQueue<>(bufferSize);
            }

            void cancel() {
                SubscriptionHelper.cancel(this);
            }

            @Override
            public void onSubscribe(Flow.Subscription s) {
                if (SubscriptionHelper.replace(this, s)) {
                    s.request(bufferSize);
                }
            }

            @Override
            public void onNext(R t) {
                queue.offer(t);
                parent.drain();
            }

            @Override
            public void onError(Throwable t) {
                parent.innerError(t);
            }

            @Override
            public void onComplete() {
                done = true;
                parent.drain();
            }

            void produced(long f) {
                long p = produced + f;
                if (p >= limit) {
                    produced = 0;
                    get().request(p);
                } else {
                    produced = p;
                }
            }
        }
    }
}
