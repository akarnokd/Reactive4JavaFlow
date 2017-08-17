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
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

/**
 * Given sorted rail sequences (according to the provided comparator) as List
 * emit the smallest item from these parallel Lists to the Subscriber.
 * <p>
 * It expects the source to emit exactly one list (which could be empty).
 *
 * @param <T> the value type
 */
public final class ParallelSortedJoin<T> extends Folyam<T> {

    final ParallelFolyam<List<T>> source;

    final Comparator<? super T> comparator;

    public ParallelSortedJoin(ParallelFolyam<List<T>> source, Comparator<? super T> comparator) {
        this.source = source;
        this.comparator = comparator;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        SortedJoinSubscription<T> parent = new SortedJoinSubscription<>(s, source.parallelism(), comparator);
        s.onSubscribe(parent);

        source.subscribe(parent.subscribers);
    }

    static final class SortedJoinSubscription<T>
    extends AtomicInteger
    implements Flow.Subscription {

        private static final long serialVersionUID = 3481980673745556697L;

        final FolyamSubscriber<? super T> actual;

        final SortedJoinInnerSubscriber<T>[] subscribers;

        final List<T>[] lists;

        final int[] indexes;

        final Comparator<? super T> comparator;

        final AtomicLong requested = new AtomicLong();

        volatile boolean cancelled;

        final AtomicInteger remaining = new AtomicInteger();

        final AtomicReference<Throwable> error = new AtomicReference<>();

        @SuppressWarnings("unchecked")
        SortedJoinSubscription(FolyamSubscriber<? super T> actual, int n, Comparator<? super T> comparator) {
            this.actual = actual;
            this.comparator = comparator;

            SortedJoinInnerSubscriber<T>[] s = new SortedJoinInnerSubscriber[n];

            for (int i = 0; i < n; i++) {
                s[i] = new SortedJoinInnerSubscriber<>(this, i);
            }
            this.subscribers = s;
            this.lists = new List[n];
            this.indexes = new int[n];
            remaining.lazySet(n);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(requested, n);
            if (remaining.get() == 0) {
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelAll();
                if (getAndIncrement() == 0) {
                    Arrays.fill(lists, null);
                }
            }
        }

        void cancelAll() {
            for (SortedJoinInnerSubscriber<T> s : subscribers) {
                s.cancel();
            }
        }

        void innerNext(List<T> value, int index) {
            lists[index] = value;
            if (remaining.decrementAndGet() == 0) {
                drain();
            }
        }

        void innerError(Throwable e) {
            if (error.compareAndSet(null, e)) {
                drain();
            } else {
                if (e != error.get()) {
                    FolyamPlugins.onError(e);
                }
            }
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            FolyamSubscriber<? super T> a = actual;
            List<T>[] lists = this.lists;
            int[] indexes = this.indexes;
            int n = indexes.length;

            for (;;) {

                long r = requested.get();
                long e = 0L;

                while (e != r) {
                    if (cancelled) {
                        Arrays.fill(lists, null);
                        return;
                    }

                    Throwable ex = error.get();
                    if (ex != null) {
                        cancelAll();
                        Arrays.fill(lists, null);
                        a.onError(ex);
                        return;
                    }

                    T min = null;
                    int minIndex = -1;

                    for (int i = 0; i < n; i++) {
                        List<T> list = lists[i];
                        int index = indexes[i];

                        if (list.size() != index) {
                            if (min == null) {
                                min = list.get(index);
                                minIndex = i;
                            } else {
                                T b = list.get(index);

                                boolean smaller;

                                try {
                                    smaller = comparator.compare(min, b) > 0;
                                } catch (Throwable exc) {
                                    FolyamPlugins.handleFatal(exc);
                                    cancelAll();
                                    Arrays.fill(lists, null);
                                    if (!error.compareAndSet(null, exc)) {
                                        FolyamPlugins.onError(exc);
                                    }
                                    a.onError(error.get());
                                    return;
                                }
                                if (smaller) {
                                    min = b;
                                    minIndex = i;
                                }
                            }
                        }
                    }

                    if (min == null) {
                        Arrays.fill(lists, null);
                        a.onComplete();
                        return;
                    }

                    a.onNext(min);

                    indexes[minIndex]++;

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        Arrays.fill(lists, null);
                        return;
                    }

                    Throwable ex = error.get();
                    if (ex != null) {
                        cancelAll();
                        Arrays.fill(lists, null);
                        a.onError(ex);
                        return;
                    }

                    boolean empty = true;

                    for (int i = 0; i < n; i++) {
                        if (indexes[i] != lists[i].size()) {
                            empty = false;
                            break;
                        }
                    }

                    if (empty) {
                        Arrays.fill(lists, null);
                        a.onComplete();
                        return;
                    }
                }

                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                int w = get();
                if (w == missed) {
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }

    static final class SortedJoinInnerSubscriber<T>
    extends AtomicReference<Flow.Subscription>
    implements FolyamSubscriber<List<T>> {


        private static final long serialVersionUID = 6751017204873808094L;

        final SortedJoinSubscription<T> parent;

        final int index;

        SortedJoinInnerSubscriber(SortedJoinSubscription<T> parent, int index) {
            this.parent = parent;
            this.index = index;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            if (SubscriptionHelper.replace(this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(List<T> t) {
            parent.innerNext(t, index);
        }

        @Override
        public void onError(Throwable t) {
            parent.innerError(t);
        }

        @Override
        public void onComplete() {
            // ignored
        }

        void cancel() {
            SubscriptionHelper.cancel(this);
        }
    }
}
