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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

/**
 * Dispatches the values from upstream in a round robin fashion to subscribers which are
 * ready to consume elements. A value from upstream is sent to only one of the subscribers.
 *
 * @param <T> the value type
 */
public final class ParallelFromPublisher<T> extends ParallelFolyam<T> {
    final Flow.Publisher<? extends T> source;

    final int parallelism;

    final int prefetch;

    public ParallelFromPublisher(Flow.Publisher<? extends T> source, int parallelism, int prefetch) {
        this.source = source;
        this.parallelism = parallelism;
        this.prefetch = prefetch;
    }

    @Override
    public int parallelism() {
        return parallelism;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super T>[] subscribers) {
        source.subscribe(new ParallelDispatcher<T>(subscribers, prefetch));
    }

    static final class ParallelDispatcher<T>
    extends AtomicInteger
    implements FolyamSubscriber<T> {

        private static final long serialVersionUID = -4470634016609963609L;

        final FolyamSubscriber<? super T>[] subscribers;

        final AtomicLongArray requests;

        final long[] emissions;

        final int prefetch;

        final int limit;

        Flow.Subscription s;

        FusedQueue<T> queue;

        Throwable error;

        volatile boolean done;

        int index;

        volatile boolean cancelled;

        /**
         * Counts how many subscribers were setup to delay triggering the
         * drain of upstream until all of them have been setup.
         */
        final AtomicInteger subscriberCount = new AtomicInteger();

        int produced;

        int sourceMode;

        ParallelDispatcher(FolyamSubscriber<? super T>[] subscribers, int prefetch) {
            this.subscribers = subscribers;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            int m = subscribers.length;
            this.requests = new AtomicLongArray(m + m + 1);
            this.requests.lazySet(m + m, m);
            this.emissions = new long[m];
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            if (s instanceof FusedSubscription) {
                @SuppressWarnings("unchecked")
                FusedSubscription<T> qs = (FusedSubscription<T>) s;

                int m = qs.requestFusion(FusedSubscription.ANY);

                if (m == FusedSubscription.SYNC) {
                    sourceMode = m;
                    queue = qs;
                    done = true;
                    setupSubscribers();
                    drain();
                    return;
                } else
                if (m == FusedSubscription.ASYNC) {
                    sourceMode = m;
                    queue = qs;

                    setupSubscribers();

                    s.request(prefetch);

                    return;
                }
            }

            queue = new SpscArrayQueue<T>(prefetch);

            setupSubscribers();

            s.request(prefetch);
        }

        void setupSubscribers() {
            FolyamSubscriber<? super T>[] subs = subscribers;
            final int m = subs.length;

            for (int i = 0; i < m; i++) {
                if (cancelled) {
                    return;
                }

                subscriberCount.lazySet(i + 1);

                subs[i].onSubscribe(new RailSubscription(i, m));
            }
        }

        final class RailSubscription implements Flow.Subscription {

            final int j;

            final int m;

            RailSubscription(int j, int m) {
                this.j = j;
                this.m = m;
            }

            @Override
            public void request(long n) {
                AtomicLongArray ra = requests;
                for (;;) {
                    long r = ra.get(j);
                    if (r == Long.MAX_VALUE) {
                        return;
                    }
                    long u = SubscriptionHelper.addCap(r, n);
                    if (ra.compareAndSet(j, r, u)) {
                        break;
                    }
                }
                if (subscriberCount.get() == m) {
                    drain();
                }
            }

            @Override
            public void cancel() {
                if (requests.compareAndSet(m + j, 0L, 1L)) {
                    ParallelDispatcher.this.cancel(m + m);
                }
            }
        }

        @Override
        public void onNext(T t) {
            if (sourceMode == FusedSubscription.NONE) {
                if (!queue.offer(t)) {
                    s.cancel();
                    onError(new IllegalStateException("Queue is full?"));
                    return;
                }
            }
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        void cancel(int m) {
            if (requests.decrementAndGet(m) == 0L) {
                cancelled = true;
                this.s.cancel();

                if (getAndIncrement() == 0) {
                    queue.clear();
                }
            }
        }

        void drainAsync() {
            int missed = 1;

            FusedQueue<T> q = queue;
            FolyamSubscriber<? super T>[] a = this.subscribers;
            AtomicLongArray r = this.requests;
            long[] e = this.emissions;
            int n = e.length;
            int idx = index;
            int consumed = produced;

            for (;;) {

                int notReady = 0;

                for (;;) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = done;
                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();
                            for (FolyamSubscriber<? super T> s : a) {
                                s.onError(ex);
                            }
                            return;
                        }
                    }

                    boolean empty = q.isEmpty();

                    if (d && empty) {
                        for (FolyamSubscriber<? super T> s : a) {
                            s.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    long requestAtIndex = r.get(idx);
                    long emissionAtIndex = e[idx];
                    if (requestAtIndex != emissionAtIndex && r.get(n + idx) == 0) {

                        T v;

                        try {
                            v = q.poll();
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            s.cancel();
                            for (FolyamSubscriber<? super T> s : a) {
                                s.onError(ex);
                            }
                            return;
                        }

                        if (v == null) {
                            break;
                        }

                        a[idx].onNext(v);

                        e[idx] = emissionAtIndex + 1;

                        int c = ++consumed;
                        if (c == limit) {
                            consumed = 0;
                            s.request(c);
                        }
                        notReady = 0;
                    } else {
                        notReady++;
                    }

                    idx++;
                    if (idx == n) {
                        idx = 0;
                    }

                    if (notReady == n) {
                        break;
                    }
                }

                int w = get();
                if (w == missed) {
                    index = idx;
                    produced = consumed;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }

        void drainSync() {
            int missed = 1;

            FusedQueue<T> q = queue;
            FolyamSubscriber<? super T>[] a = this.subscribers;
            AtomicLongArray r = this.requests;
            long[] e = this.emissions;
            int n = e.length;
            int idx = index;

            for (;;) {

                int notReady = 0;

                for (;;) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean empty = q.isEmpty();

                    if (empty) {
                        for (FolyamSubscriber<? super T> s : a) {
                            s.onComplete();
                        }
                        return;
                    }

                    long requestAtIndex = r.get(idx);
                    long emissionAtIndex = e[idx];
                    if (requestAtIndex != emissionAtIndex && r.get(n + idx) == 0) {

                        T v;

                        try {
                            v = q.poll();
                        } catch (Throwable ex) {
                            FolyamPlugins.handleFatal(ex);
                            s.cancel();
                            for (FolyamSubscriber<? super T> s : a) {
                                s.onError(ex);
                            }
                            return;
                        }

                        if (v == null) {
                            for (FolyamSubscriber<? super T> s : a) {
                                s.onComplete();
                            }
                            return;
                        }

                        a[idx].onNext(v);

                        e[idx] = emissionAtIndex + 1;

                        notReady = 0;
                    } else {
                        notReady++;
                    }

                    idx++;
                    if (idx == n) {
                        idx = 0;
                    }

                    if (notReady == n) {
                        break;
                    }
                }

                int w = get();
                if (w == missed) {
                    index = idx;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            if (sourceMode == FusedSubscription.SYNC) {
                drainSync();
            } else {
                drainAsync();
            }
        }
    }
}
