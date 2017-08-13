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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamGroupBy<T, K, V> extends Folyam<GroupedFolyam<K, V>> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends K> keySelector;

    final CheckedFunction<? super T, ? extends V> valueSelector;

    final int prefetch;

    public FolyamGroupBy(Folyam<T> source, CheckedFunction<? super T, ? extends K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector, int prefetch) {
        this.source = source;
        this.keySelector = keySelector;
        this.valueSelector = valueSelector;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super GroupedFolyam<K, V>> s) {
        source.subscribe(new GroupBySubscriber<>(s, keySelector, valueSelector, prefetch));
    }

    static final class GroupBySubscriber<T, K, V> extends AtomicInteger implements FolyamSubscriber<T>, FusedSubscription<GroupedFolyam<K, V>> {

        final FolyamSubscriber<? super GroupedFolyam<K, V>> actual;

        final CheckedFunction<? super T, ? extends K> keySelector;

        final CheckedFunction<? super T, ? extends V> valueSelector;

        final int prefetch;

        final ConcurrentMap<K, SolocastGroup<K, V>> groups;

        final SpscLinkedArrayQueue<SolocastGroup<K, V>> queue;

        Flow.Subscription upstream;

        boolean outputFused;

        long requested;
        static final VarHandle REQUESTED;

        boolean cancelled;
        static final VarHandle CANCELLED;

        boolean done;
        static final VarHandle DONE;

        int groupCount;
        static final VarHandle GROUP_COUNT;

        Throwable error;

        long emitted;

        static {
            try {
                CANCELLED = MethodHandles.lookup().findVarHandle(GroupBySubscriber.class, "cancelled", boolean.class);
                DONE = MethodHandles.lookup().findVarHandle(GroupBySubscriber.class, "done", boolean.class);
                GROUP_COUNT = MethodHandles.lookup().findVarHandle(GroupBySubscriber.class, "groupCount", int.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(GroupBySubscriber.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        GroupBySubscriber(FolyamSubscriber<? super GroupedFolyam<K, V>> actual, CheckedFunction<? super T, ? extends K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector, int prefetch) {
            this.actual = actual;
            this.keySelector = keySelector;
            this.valueSelector = valueSelector;
            this.prefetch = prefetch;
            this.groups = new ConcurrentHashMap<>();
            this.queue = new SpscLinkedArrayQueue<>(prefetch);
            GROUP_COUNT.setRelease(this, 1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(prefetch);
        }

        @Override
        public void onNext(T item) {
            K key;
            V value;

            try {
                key = Objects.requireNonNull(keySelector.apply(item), "The keySelector returned a null value");
                value = Objects.requireNonNull(valueSelector.apply(item), "The valueSelector returned a null value");
            } catch (Throwable ex) {
                upstream.cancel();
                onError(ex);
                return;
            }

            SolocastGroup<K, V> g = groups.get(key);
            if (g != null) {
                g.onNext(value);
            } else
            if (!(boolean)CANCELLED.getAcquire(this)) {
                g = new SolocastGroup<>(key, this);
                g.onNext(value);
                GROUP_COUNT.getAndAdd(this, 1);
                groups.put(key, g);
                queue.offer(g);
                drain();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            for (SolocastGroup<K, V> g : groups.values()) {
                g.onError(throwable);
            }
            groups.clear();
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void onComplete() {
            for (SolocastGroup<K, V> g : groups.values()) {
                g.onComplete();
            }
            groups.clear();
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
            if (CANCELLED.compareAndSet(this, false, true)) {
                tryCancel();
            }
        }

        void tryCancel() {
            if ((int)GROUP_COUNT.getAndAdd(this, -1) - 1 == 0) {
                upstream.cancel();
                groups.clear();
                if (getAndIncrement() == 0) {
                    queue.clear();
                }
            }
        }

        void removeGroup(K key) {
            groups.remove(key);
            tryCancel();
        }

        void requestGroup(long n) {
            upstream.request(n);
        }

        void drain() {
            if (getAndIncrement() == 0) {
                if (outputFused) {
                    drainFused();
                } else {
                    drainNormal();
                }
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
        public GroupedFolyam<K, V> poll() throws Throwable {
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

        void drainFused() {
            int missed = 1;
            FolyamSubscriber<? super GroupedFolyam<K, V>> a = actual;
            PlainQueue<?> q = queue;

            for (;;) {
                if ((int)GROUP_COUNT.getAcquire(this) == 0) {
                    q.clear();
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                if (!q.isEmpty()) {
                    a.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        a.onComplete();
                    } else {
                        a.onError(ex);
                    }
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        void drainNormal() {
            int missed = 1;
            FolyamSubscriber<? super GroupedFolyam<K, V>> a = actual;
            PlainQueue<SolocastGroup<K, V>> q = queue;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    SolocastGroup<K, V> g = q.poll();
                    boolean empty = g == null;

                    if (d && empty) {
                        Throwable ex = error;
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

                    a.onNext(g);

                    e++;
                }

                if (e == r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        q.clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = q.isEmpty();

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        static final class SolocastGroup<K, V> extends GroupedFolyam<K, V> implements FusedSubscription<V> {

            final K key;

            final GroupBySubscriber<?, K, V> parent;

            final PlainQueue<V> queue;

            final int limit;

            boolean outputFused;

            boolean once;
            static final VarHandle ONCE;

            FolyamSubscriber<? super V> actual;
            static final VarHandle ACTUAL;

            long requested;
            static final VarHandle REQUESTED;

            int wip;
            static final VarHandle WIP;

            boolean done;
            static final VarHandle DONE;
            Throwable error;

            boolean cancelled;
            static final VarHandle CANCELLED;

            long emitted;

            int consumed;

            static {
                try {
                    ONCE = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "once", boolean.class);
                    WIP = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "wip", int.class);
                    REQUESTED = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "requested", long.class);
                    DONE = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "done", boolean.class);
                    ACTUAL = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "actual", FolyamSubscriber.class);
                    CANCELLED = MethodHandles.lookup().findVarHandle(SolocastGroup.class, "cancelled", boolean.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            SolocastGroup(K key, GroupBySubscriber<?, K, V> parent) {
                this.key = key;
                this.parent = parent;
                int p = parent.prefetch;
                this.limit = p - (p >> 2);
                this.queue = new SpscArrayQueue<>(p);
            }

            @Override
            public K getKey() {
                return key;
            }

            @Override
            protected void subscribeActual(FolyamSubscriber<? super V> s) {
                if (ONCE.compareAndSet(this, false, true)) {
                    s.onSubscribe(this);
                    ACTUAL.setVolatile(this, s);
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        ACTUAL.set(this, null);
                    } else {
                        drain();
                    }
                } else {
                    EmptySubscription.error(s, new IllegalStateException("Only one subscriber allowed"));
                }
            }

            @Override
            public void request(long n) {
                SubscriptionHelper.addRequested(this, REQUESTED, n);
                drain();
            }

            @Override
            public void cancel() {
                if (CANCELLED.compareAndSet(this, false, true)) {
                    parent.removeGroup(key);
                    if ((int)WIP.getAndAdd(this, 1) == 0) {
                        ACTUAL.set(this, null);
                        cleanup(queue);
                    }
                }
            }

            void onNext(V item) {
                if (!(boolean) DONE.getAcquire(this) && !(boolean) CANCELLED.getAcquire(this)) {
                    queue.offer(item);
                    drain();
                }
            }

            void onError(Throwable ex) {
                if (!(boolean) DONE.getAcquire(this) && !(boolean) CANCELLED.getAcquire(this)) {
                    error = ex;
                    DONE.setRelease(this, true);
                    drain();
                }
            }

            void onComplete() {
                if (!(boolean) DONE.getAcquire(this) && !(boolean) CANCELLED.getAcquire(this)) {
                    DONE.setRelease(this, true);
                    drain();
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
            public V poll() throws Throwable {
                V v = queue.poll();
                if (v != null) {
                    int c = consumed + 1;
                    if (c == limit) {
                        consumed = 0;
                        parent.requestGroup(c);
                    } else {
                        consumed = c;
                    }
                }
                return v;
            }

            @Override
            public boolean isEmpty() {
                return queue.isEmpty();
            }

            @Override
            public void clear() {
                cleanup(queue);
            }

            void drain() {
                if ((int)WIP.getAndAdd(this, 1) == 0) {
                    int missed = 1;
                    for (;;) {
                        FolyamSubscriber<? super V> a = (FolyamSubscriber<? super V>) ACTUAL.getAcquire(this);

                        if (a != null) {
                            if (outputFused) {
                                drainFused(a);
                            } else {
                                drainNormal(a);
                            }
                            return;
                        }

                        missed = (int)WIP.getAndAdd(this, -missed) - missed;
                        if (missed == 0) {
                            break;
                        }
                    }
                }
            }

            void cleanup(PlainQueue<?> q) {
                long c = 0;
                while (q.poll() != null) {
                    c++;
                }
                parent.requestGroup(c);
            }

            void drainFused(FolyamSubscriber<? super V> a) {
                int missed = 1;
                PlainQueue<?> q = queue;

                for (;;) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        ACTUAL.set(this, null);
                        cleanup(q);
                        return;
                    }

                    boolean d = (boolean) DONE.getAcquire(this);

                    if (!q.isEmpty()) {
                        a.onNext(null);
                    }

                    if (d) {
                        ACTUAL.set(this, null);
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    missed = (int)WIP.getAndAdd(this, -missed) - missed;
                    if (missed == 0) {
                        break;
                    }
                }
            }

            void drainNormal(FolyamSubscriber<? super V> a) {
                int missed = 1;
                PlainQueue<V> q = queue;
                long e = emitted;
                int c = consumed;
                int lim = limit;

                for (;;) {

                    long r = (long)REQUESTED.getAcquire(this);

                    while (e != r) {
                        if ((boolean)CANCELLED.getAcquire(this)) {
                            ACTUAL.set(this, null);
                            cleanup(q);
                            return;
                        }

                        boolean d = (boolean)DONE.getAcquire(this);
                        V v = q.poll();
                        boolean empty = v == null;

                        if (d && empty) {
                            Throwable ex = error;
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

                        a.onNext(v);

                        e++;

                        if (++c == lim) {
                            c = 0;
                            parent.requestGroup(lim);
                        }
                    }

                    if (e == r) {
                        if ((boolean)CANCELLED.getAcquire(this)) {
                            q.clear();
                            return;
                        }

                        boolean d = (boolean)DONE.getAcquire(this);
                        boolean empty = q.isEmpty();

                        if (d && empty) {
                            Throwable ex = error;
                            if (ex == null) {
                                a.onComplete();
                            } else {
                                a.onError(ex);
                            }
                            return;
                        }
                    }

                    consumed = c;
                    emitted = e;
                    missed = (int)WIP.getAndAdd(this, -missed) - missed;
                    if (missed == 0) {
                        break;
                    }
                }

            }
        }
    }
}
