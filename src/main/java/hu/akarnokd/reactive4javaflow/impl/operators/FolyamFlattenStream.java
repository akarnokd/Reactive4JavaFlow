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
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public final class FolyamFlattenStream<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Stream<? extends R>> mapper;

    final int prefetch;

    public FolyamFlattenStream(Folyam<T> source, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper, int prefetch) {
        this.source = source;
        this.mapper = mapper;
        this.prefetch = prefetch;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FlattenIterableConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, prefetch));
        } else {
            source.subscribe(new FlattenIterableSubscriber<>(s, mapper, prefetch));
        }
    }

    static abstract class AbstractFlattenIterable<T, R> extends AtomicInteger implements FusedSubscription<R>, FolyamSubscriber<T> {

        final CheckedFunction<? super T, ? extends Stream<? extends R>> mapper;

        final int prefetch;

        final int limit;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractFlattenIterable.class, "requested", Long.TYPE);

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), AbstractFlattenIterable.class, "done", Boolean.TYPE);
        Throwable error;

        volatile boolean cancelled;

        Flow.Subscription upstream;

        FusedQueue<T> queue;

        Iterator<? extends R> current;
        Stream<? extends R> currentStream;

        int sourceFused;
        int outputFused;

        int consumed;

        long emitted;

        protected AbstractFlattenIterable(CheckedFunction<? super T, ? extends Stream<? extends R>> mapper, int prefetch) {
            this.mapper = mapper;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
        }

        @SuppressWarnings("unchecked")
        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                FusedSubscription<T> fs = (FusedSubscription<T>) subscription;
                int m = fs.requestFusion(ANY);
                if (m == SYNC) {
                    sourceFused = m;
                    queue = fs;
                    DONE.setRelease(this, true);
                    onStart();
                    return;
                }
                if (m == ASYNC) {
                    sourceFused = m;
                    queue = fs;
                    onStart();
                    fs.request(prefetch);
                    return;
                }
            }

            int pf = prefetch;
            if (pf == 1) {
                queue = new SpscOneQueue<>();
            } else {
                queue = new SpscArrayQueue<>(pf);
            }

            onStart();

            subscription.request(pf);
        }

        abstract void onStart();

        @Override
        public final R poll() throws Throwable {
            Iterator<? extends R> it = current;
            for (;;) {
                if (it == null) {
                    T v = queue.poll();
                    if (v != null) {

                        Stream<? extends R> s = mapper.apply(v);
                        it = s.iterator();

                        if (it.hasNext()) {
                            current = it;
                            currentStream = s;
                            return Objects.requireNonNull(it.next(), "The iterator returned a null value");
                        }
                        it = null;
                        DisposableHelper.closeSilently(s);
                        continue;
                    }
                    return null;
                }
                if (it.hasNext()) {
                    return Objects.requireNonNull(it.next(), "The iterator returned a null value");
                }
                it = null;
                closeCurrent();
            }

        }

        @Override
        public final boolean isEmpty() {
            return current == null && queue.isEmpty();
        }

        @Override
        public final void clear() {
            closeCurrent();
            queue.clear();
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        void closeCurrent() {
            current = null;
            Stream<? extends R> s = currentStream;
            currentStream = null;
            DisposableHelper.closeSilently(s);
        }

        @Override
        public final int requestFusion(int mode) {
            if (sourceFused == SYNC && (mode & SYNC) != 0) {
                outputFused = SYNC;
                return SYNC;
            }
            return NONE;
        }

        @Override
        public final void onNext(T item) {
            if (item != null) {
                queue.offer(item);
            }
            drain();
        }

        @Override
        public final void onError(Throwable throwable) {
            error = throwable;
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public final void cancel() {
            cancelled = true;
            upstream.cancel();
            if (getAndIncrement() == 0) {
                clear();
            }
        }

        final void drain() {
            if (getAndIncrement() == 0) {
                if (sourceFused == SYNC) {
                    drainSync();
                } else {
                    drainNormal();
                }
            }
        }

        abstract void drainSync();

        abstract void drainNormal();
    }

    static final class FlattenIterableSubscriber<T, R> extends AbstractFlattenIterable<T, R> {

        final FolyamSubscriber<? super R> actual;

        protected FlattenIterableSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper, int prefetch) {
            super(mapper, prefetch);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainSync() {
            FolyamSubscriber<? super R> a = actual;
            FusedQueue<T> q = queue;
            Iterator<? extends R> it = current;
            int missed = 1;
            long e = emitted;

            outer:
            for (;;) {

                if (it == null) {

                    if (cancelled) {
                        clear();
                        return;
                    }

                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    boolean empty = v == null;

                    if (empty) {
                        a.onComplete();
                        return;
                    }

                    Stream<? extends R> s;
                    try {
                        s = mapper.apply(v);
                        it = s.iterator();
                        if (!it.hasNext()) {
                            it = null;
                            DisposableHelper.closeSilently(s);
                            continue;
                        }
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }
                    current = it;
                    currentStream = s;
                }

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clear();
                        return;
                    }

                    R v;

                    try {
                        v = it.next();
                    } catch (Throwable ex) {
                        closeCurrent();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (cancelled) {
                        clear();
                        return;
                    }

                    a.onNext(v);
                    e++;

                    if (cancelled) {
                        clear();
                        return;
                    }

                    boolean has;

                    try {
                        has = it.hasNext();
                    } catch (Throwable ex) {
                        closeCurrent();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (!has) {
                        it = null;
                        closeCurrent();
                        continue outer;
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainNormal() {
            FolyamSubscriber<? super R> a = actual;
            FusedQueue<T> q = queue;
            Iterator<? extends R> it = current;
            int missed = 1;
            long e = emitted;
            int lim = limit;

            outer:
            for (;;) {

                if (it == null) {

                    if (cancelled) {
                        clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

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

                    if (!empty) {

                        int c = consumed + 1;
                        if (c == lim) {
                            consumed = 0;
                            upstream.request(lim);
                        } else {
                            consumed = c;
                        }

                        Stream<? extends R> s;
                        try {
                            s = mapper.apply(v);
                            it = s.iterator();
                            if (!it.hasNext()) {
                                it = null;
                                DisposableHelper.closeSilently(s);
                                continue;
                            }
                        } catch (Throwable ex) {
                            upstream.cancel();
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                        current = it;
                        currentStream = s;
                    }
                }

                if (it != null) {
                    long r = (long)REQUESTED.getAcquire(this);

                    while (e != r) {
                        if (cancelled) {
                            clear();
                            return;
                        }

                        R v;

                        try {
                            v = it.next();
                        } catch (Throwable ex) {
                            closeCurrent();
                            q.clear();
                            a.onError(ex);
                            return;
                        }

                        if (cancelled) {
                            clear();
                            return;
                        }

                        a.onNext(v);
                        e++;

                        if (cancelled) {
                            clear();
                            return;
                        }

                        boolean has;

                        try {
                            has = it.hasNext();
                        } catch (Throwable ex) {
                            closeCurrent();
                            q.clear();
                            a.onError(ex);
                            return;
                        }

                        if (!has) {
                            it = null;
                            closeCurrent();
                            continue outer;
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

    static final class FlattenIterableConditionalSubscriber<T, R> extends AbstractFlattenIterable<T, R> {

        final ConditionalSubscriber<? super R> actual;

        protected FlattenIterableConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper, int prefetch) {
            super(mapper, prefetch);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        void drainSync() {
            ConditionalSubscriber<? super R> a = actual;
            FusedQueue<T> q = queue;
            Iterator<? extends R> it = current;
            int missed = 1;
            long e = emitted;

            outer:
            for (;;) {

                if (it == null) {

                    if (cancelled) {
                        clear();
                        return;
                    }

                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    boolean empty = v == null;

                    if (empty) {
                        a.onComplete();
                        return;
                    }

                    Stream<? extends R> s;
                    try {
                        s = mapper.apply(v);
                        it = s.iterator();
                        if (!it.hasNext()) {
                            it = null;
                            DisposableHelper.closeSilently(s);
                            continue;
                        }
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }
                    current = it;
                    currentStream = s;
                }

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clear();
                        return;
                    }

                    R v;

                    try {
                        v = it.next();
                    } catch (Throwable ex) {
                        closeCurrent();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (cancelled) {
                        clear();
                        return;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    if (cancelled) {
                        clear();
                        return;
                    }

                    boolean has;

                    try {
                        has = it.hasNext();
                    } catch (Throwable ex) {
                        closeCurrent();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (!has) {
                        it = null;
                        closeCurrent();
                        continue outer;
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainNormal() {
            ConditionalSubscriber<? super R> a = actual;
            FusedQueue<T> q = queue;
            Iterator<? extends R> it = current;
            int missed = 1;
            long e = emitted;
            int lim = limit;

            outer:
            for (;;) {

                if (it == null) {

                    if (cancelled) {
                        clear();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    T v;

                    try {
                        v = q.poll();
                    } catch (Throwable ex) {
                        upstream.cancel();
                        q.clear();
                        a.onError(ex);
                        return;
                    }

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

                    if (!empty) {

                        int c = consumed + 1;
                        if (c == lim) {
                            consumed = 0;
                            upstream.request(lim);
                        } else {
                            consumed = c;
                        }

                        Stream<? extends R> s;
                        try {
                            s = mapper.apply(v);
                            it = s.iterator();
                            if (!it.hasNext()) {
                                it = null;
                                DisposableHelper.closeSilently(s);
                                continue;
                            }
                        } catch (Throwable ex) {
                            upstream.cancel();
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                        current = it;
                        currentStream = s;
                    }
                }

                if (it != null) {
                    long r = (long)REQUESTED.getAcquire(this);

                    while (e != r) {
                        if (cancelled) {
                            clear();
                            return;
                        }

                        R v;

                        try {
                            v = it.next();
                        } catch (Throwable ex) {
                            closeCurrent();
                            q.clear();
                            a.onError(ex);
                            return;
                        }

                        if (cancelled) {
                            clear();
                            return;
                        }

                        if (a.tryOnNext(v)) {
                            e++;
                        }

                        if (cancelled) {
                            clear();
                            return;
                        }

                        boolean has;

                        try {
                            has = it.hasNext();
                        } catch (Throwable ex) {
                            closeCurrent();
                            q.clear();
                            a.onError(ex);
                            return;
                        }

                        if (!has) {
                            it = null;
                            closeCurrent();
                            continue outer;
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
