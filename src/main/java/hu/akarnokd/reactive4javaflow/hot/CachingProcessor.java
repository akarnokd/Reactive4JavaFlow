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

package hu.akarnokd.reactive4javaflow.hot;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.lang.reflect.Array;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class CachingProcessor<T> extends FolyamProcessor<T> implements AutoDisposable {

    final BufferManager<T> manager;

    CachingSubscription<T>[] subscribers;
    static final VarHandle SUBSCRIBERS;

    static final CachingSubscription[] EMPTY = new CachingSubscription[0];
    static final CachingSubscription[] TERMINATED = new CachingSubscription[0];

    Throwable error;
    static final VarHandle ERROR;

    Flow.Subscription upstream;
    static final VarHandle UPSTREAM;

    static {
        try {
            SUBSCRIBERS = MethodHandles.lookup().findVarHandle(CachingProcessor.class, "subscribers", CachingSubscription[].class);
            UPSTREAM = MethodHandles.lookup().findVarHandle(CachingProcessor.class, "upstream", Flow.Subscription.class);
            ERROR = MethodHandles.lookup().findVarHandle(CachingProcessor.class, "error", Throwable.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public static <T> CachingProcessor<T> withCapacityHint(int capacityHint) {
        return new CachingProcessor<>(new UnboundedBufferManager<>(capacityHint));
    }

    public CachingProcessor() {
        this(new UnboundedBufferManager<>(16));
    }

    public CachingProcessor(int maxSize) {
        this(new SizeBoundBufferManager<>(maxSize));
    }

    public CachingProcessor(long timeout, TimeUnit unit, SchedulerService executor) {
        this(new TimeBoundBufferManager<>(Integer.MAX_VALUE, timeout, unit, executor));
    }

    public CachingProcessor(int maxSize, long timeout, TimeUnit unit, SchedulerService executor) {
        this(new TimeBoundBufferManager<>(maxSize, timeout, unit, executor));
    }

    CachingProcessor(BufferManager<T> manager) {
        this.manager = manager;
        SUBSCRIBERS.setRelease(this, EMPTY);
    }

    @Override
    public boolean hasThrowable() {
        Throwable ex = (Throwable)ERROR.getAcquire(this);
        return ex != null && ex != ExceptionHelper.TERMINATED;
    }

    @Override
    public Throwable getThrowable() {
        Throwable ex = (Throwable)ERROR.getAcquire(this);
        return ex != ExceptionHelper.TERMINATED ? ex : null;
    }

    @Override
    public boolean hasComplete() {
        return ERROR.getAcquire(this) == ExceptionHelper.TERMINATED;
    }

    @Override
    public boolean hasSubscribers() {
        return ((CachingSubscription<?>[])SUBSCRIBERS.getAcquire(this)).length != 0;
    }

    public boolean hasValues() {
        return manager.hasValues();
    }

    public T[] getValues(T[] array) {
        return manager.toArray(array);
    }

    public boolean hasTerminated() {
        return ERROR.getAcquire(this) != null;
    }

    @SuppressWarnings("unchecked")
    boolean add(CachingSubscription<T> ds) {
        for (;;) {
            CachingSubscription<T>[] a = (CachingSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            CachingSubscription<T>[] b = new CachingSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = ds;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(CachingSubscription<T> ds) {
        for (;;) {
            CachingSubscription<T>[] a = (CachingSubscription<T>[])SUBSCRIBERS.getAcquire(this);
            int n = a.length;
            if (n == 0) {
                return;
            }
            int j = -1;
            for (int i = 0; i < n; i++) {
                if (ds == a[i]) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                break;
            }
            CachingSubscription<T>[] b;
            if (n == 1) {
                b = EMPTY;
            } else {
                b = new CachingSubscription[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                break;
            }
        }
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        CachingSubscription<T> parent = new CachingSubscription<>(s, manager, this);
        s.onSubscribe(parent);
        if (add(parent)) {
            if (parent.isCancelled()) {
                remove(parent);
                return;
            }
        }
        manager.replay(parent);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
            subscription.request(Long.MAX_VALUE);
        }
    }

    public boolean prepare(Flow.Subscription subscription) {
        return UPSTREAM.compareAndSet(this, null, subscription);
    }

    @Override
    public void onNext(T item) {
        if (ERROR.getAcquire(this) == null) {
            BufferManager<T> bm = manager;
            bm.onNext(item);
            for (CachingSubscription<T> cs : (CachingSubscription<T>[])SUBSCRIBERS.getAcquire(this)) {
                bm.replay(cs);
            }
        }
    }

    boolean tryOnError(Throwable throwable) {
        if (ERROR.getAcquire(this) == null && ERROR.compareAndSet(this, null, throwable)) {
            BufferManager<T> bm = manager;
            bm.onError(throwable);
            for (CachingSubscription<T> cs : (CachingSubscription<T>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                bm.replay(cs);
            }
            return true;
        }
        return false;
    }

    @Override
    public void onError(Throwable throwable) {
        if (!tryOnError(throwable)) {
            FolyamPlugins.onError(throwable);
        }
    }

    @Override
    public void onComplete() {
        if (ERROR.getAcquire(this) == null && ERROR.compareAndSet(this, null, ExceptionHelper.TERMINATED)) {
            BufferManager<T> bm = manager;
            bm.onComplete();
            for (CachingSubscription<T> cs : (CachingSubscription<T>[])SUBSCRIBERS.getAndSet(this, TERMINATED)) {
                bm.replay(cs);
            }
        }
    }

    @Override
    public void close() {
        SubscriptionHelper.cancel(this, UPSTREAM);
        tryOnError(new CancellationException("CachingProcessor closed"));
    }

    interface BufferManager<T> {

        void onNext(T item);

        void onError(Throwable ex);

        void onComplete();

        default void replay(CachingSubscription<T> cs) {
            if (cs.getAndIncrement() == 0) {
                if (cs.outputFused) {
                    replayFused(cs);
                } else {
                    replayNormal(cs);
                }
            }
        }

        void replayFused(CachingSubscription<T> cs);

        void replayNormal(CachingSubscription<T> cs);

        boolean hasValues();

        T[] toArray(T[] array);

        T poll(CachingSubscription<T> cs);

        boolean isEmpty(CachingSubscription<T> cs);

        void clear(CachingSubscription<T> cs);
    }

    static final class UnboundedBufferManager<T> implements BufferManager<T> {

        final int capacityHint;

        final Object[] head;

        Object[] tail;

        int tailOffset;

        long available;
        static final VarHandle AVAILABLE;

        boolean done;
        static final VarHandle DONE;

        Throwable error;

        static final VarHandle ARRAY;

        static {
            try {
                AVAILABLE = MethodHandles.lookup().findVarHandle(UnboundedBufferManager.class, "available", long.class);
                DONE = MethodHandles.lookup().findVarHandle(UnboundedBufferManager.class, "done", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        }

        UnboundedBufferManager(int capacityHint) {
            this.capacityHint = capacityHint;
            this.head = this.tail = new Object[capacityHint + 1];
            AVAILABLE.setRelease(this, 0L);
        }

        @Override
        public void onNext(T item) {
            Object[] t = tail;
            int to = tailOffset;
            int c = capacityHint;
            if (to == c) {
                Object[] b = new Object[c + 1];
                b[0] = item;
                tail = b;
                t[c] = b;
                tailOffset = 1;
            } else {
                t[to] = item;
                tailOffset = to + 1;
            }
            AVAILABLE.setVolatile(this, available + 1);
        }

        @Override
        public void onError(Throwable ex) {
            this.error = ex;
            DONE.setRelease(this, true);
        }

        @Override
        public void onComplete() {
            DONE.setRelease(this, true);
        }

        @Override
        public void replayFused(CachingSubscription<T> cs) {
            int missed = 1;

            for (;;) {

                if (cs.isCancelled()) {
                    cs.node = null;
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                if ((long)AVAILABLE.getAcquire(this) != cs.emitted) {
                    cs.actual.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        cs.actual.onComplete();
                    } else {
                        cs.actual.onError(ex);
                    }
                    return;
                }

                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public void replayNormal(CachingSubscription<T> cs) {
            int missed = 1;
            int cap = capacityHint;
            FolyamSubscriber<? super T> a = cs.actual;
            long e = cs.emitted;

            Object[] n = (Object[])cs.node;
            if (n == null) {
                n = head;
            }
            int idx = cs.nodeIndex;

            for (;;) {

                long r = cs.requested();

                for (;;) {
                    if (cs.isCancelled()) {
                        cs.node = null;
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    long avail = (long)AVAILABLE.getAcquire(this);
                    boolean empty = avail == e;
                    if (d && empty) {
                        cs.node = null;
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (empty || e == r) {
                        break;
                    }

                    if (idx == cap) {
                        n = (Object[])n[cap];
                        idx = 0;
                    }

                    a.onNext((T)n[idx++]);

                    e++;
                }

                cs.node = n;
                cs.nodeIndex = idx;
                cs.emitted = e;
                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public boolean hasValues() {
            return (long)AVAILABLE.getAcquire(this) != 0;
        }

        @Override
        public T[] toArray(T[] array) {
            long s = (long)AVAILABLE.getAcquire(this);
            if (s > array.length) {
                array = (T[])Array.newInstance(array.getClass().getComponentType(), (int)s);
            }
            Object[] n = head;
            int o = 0;
            int cap = capacityHint;
            for (int j = 0; j < s; j++) {
                if (o == cap) {
                    n = (Object[])n[cap];
                    o = 0;
                }
                array[j] = (T)n[o++];
            }

            if (s < array.length) {
                array[(int)s] = null;
            }

            return array;
        }

        @Override
        public T poll(CachingSubscription<T> cs) {
            long e = cs.emitted;
            boolean d = (boolean)DONE.getAcquire(this);
            if ((long)AVAILABLE.getAcquire(this) == e) {
                if (d) {
                    cs.node = null;
                }
                return null;
            }
            Object[] n = (Object[])cs.node;
            if (n == null) {
                n = head;
                cs.node = n;
            }
            int idx = cs.nodeIndex;
            int cap = capacityHint;
            if (idx == cap) {
                n = (Object[])n[cap];
                cs.node = n;
                idx = 0;
            }
            T v = (T)n[idx];
            cs.nodeIndex = idx + 1;
            cs.emitted = e + 1;
            return v;
        }

        @Override
        public boolean isEmpty(CachingSubscription<T> cs) {
            return (long)AVAILABLE.getAcquire(this) == cs.emitted;
        }

        @Override
        public void clear(CachingSubscription<T> cs) {
            cs.node = null;
            cs.emitted = (long)AVAILABLE.getAcquire(this);
        }
    }

    static final class SizeBoundBufferManager<T> implements BufferManager<T> {

        final int maxSize;

        Node<T> head;
        static final VarHandle HEAD;

        Node<T> tail;
        static final VarHandle TAIL;

        boolean done;
        static final VarHandle DONE;

        Throwable error;

        int size;

        static {
            try {
                HEAD = MethodHandles.lookup().findVarHandle(SizeBoundBufferManager.class, "head", Node.class);
                TAIL = MethodHandles.lookup().findVarHandle(SizeBoundBufferManager.class, "tail", Node.class);
                DONE = MethodHandles.lookup().findVarHandle(SizeBoundBufferManager.class, "done", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        SizeBoundBufferManager(int maxSize) {
            this.maxSize = maxSize;
            Node<T> n = new Node<>(null);
            tail = n;
            HEAD.setRelease(this, n);
        }

        @Override
        public void onNext(T item) {
            Node<T> n = new Node<>(item);
            tail.setRelease(n);
            TAIL.setVolatile(this, n);
            int s = size;
            if (s == maxSize) {
                HEAD.setVolatile(this, head.getPlain());
            } else {
                size = s + 1;
            }
        }

        @Override
        public void onError(Throwable ex) {
            this.error = ex;
            DONE.setRelease(this, true);
        }

        @Override
        public void onComplete() {
            DONE.setRelease(this, true);
        }

        @Override
        public void replayFused(CachingSubscription<T> cs) {
            int missed = 1;

            for (;;) {

                if (cs.isCancelled()) {
                    cs.node = null;
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                if (TAIL.getAcquire(this) != cs.node) {
                    cs.actual.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        cs.actual.onComplete();
                    } else {
                        cs.actual.onError(ex);
                    }
                    return;
                }

                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public void replayNormal(CachingSubscription<T> cs) {
            int missed = 1;
            FolyamSubscriber<? super T> a = cs.actual;
            Node<T> n = (Node<T>)cs.node;
            if (n == null) {
                n = (Node<T>)HEAD.getAcquire(this);
            }
            long e = cs.emitted;

            for (;;) {

                long r = cs.requested();

                for (;;) {

                    if (cs.isCancelled()) {
                        cs.node = null;
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = n == TAIL.getAcquire(this);

                    if (d && empty) {
                        cs.node = null;
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (empty || e == r) {
                        break;
                    }

                    Node<T> h = n.getAcquire();

                    a.onNext(h.value);

                    n = h;
                    e++;
                }

                cs.emitted = e;
                cs.node = n;
                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public boolean hasValues() {
            return ((Node<T>)HEAD.getAcquire(this)).getAcquire() != null;
        }

        @Override
        public T[] toArray(T[] array) {
            Node<T> h = (Node<T>)HEAD.getAcquire(this);
            Node<T> t = h;
            int s = 0;
            for (;;) {
                Node<T> u = t.getAcquire();
                if (u == null) {
                    break;
                }
                s++;
                t = u;
            }

            if (s > array.length) {
                array = (T[])Array.newInstance(array.getClass().getComponentType(), s);
            }

            for (int i = 0; i < s; i++) {
                Node<T> u = h.get();
                array[i] = u.value;
                h = u;
            }

            if (s < array.length) {
                array[s] = null;
            }

            return array;
        }

        @Override
        public T poll(CachingSubscription<T> cs) {
            Node<T> n = (Node<T>)cs.node;
            if (n == null) {
                n = (Node<T>)HEAD.getAcquire(this);
                cs.node = n;
            }
            boolean d = (boolean)DONE.getAcquire(this);
            Node<T> h = n.getAcquire();
            if (h == null) {
                if (d) {
                    cs.node = null;
                }
                return null;
            }
            cs.node = h;
            return h.value;
        }

        @Override
        public boolean isEmpty(CachingSubscription<T> cs) {
            return cs.node == TAIL.getAcquire(this);
        }

        @Override
        public void clear(CachingSubscription<T> cs) {
            cs.node = TAIL.getAcquire(this);
        }

        static final class Node<T> extends AtomicReference<Node<T>> {
            final T value;

            Node(T value) {
                this.value = value;
            }
        }
    }

    static final class TimeBoundBufferManager<T> implements BufferManager<T> {

        final int maxSize;

        final long timeout;

        final TimeUnit unit;

        final SchedulerService executor;

        Node<T> head;
        static final VarHandle HEAD;

        Node<T> tail;
        static final VarHandle TAIL;

        boolean done;
        static final VarHandle DONE;

        Throwable error;

        int size;

        static {
            try {
                HEAD = MethodHandles.lookup().findVarHandle(TimeBoundBufferManager.class, "head", Node.class);
                TAIL = MethodHandles.lookup().findVarHandle(TimeBoundBufferManager.class, "tail", Node.class);
                DONE = MethodHandles.lookup().findVarHandle(TimeBoundBufferManager.class, "done", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        TimeBoundBufferManager(int maxSize, long timeout, TimeUnit unit, SchedulerService executor) {
            this.maxSize = maxSize;
            this.timeout = timeout;
            this.unit = unit;
            this.executor = executor;
            Node<T> n = new Node<>(null, 0L);
            tail = n;
            HEAD.setRelease(this, n);
        }

        @Override
        public void onNext(T item) {
            Node<T> n = new Node<>(item, executor.now(unit));
            tail.setRelease(n);
            TAIL.setVolatile(this, n);
            int s = size;
            if (s == maxSize) {
                HEAD.setVolatile(this, head.getPlain());
            } else {
                size = s + 1;
            }
            findHead();
        }

        @Override
        public void onError(Throwable ex) {
            findHead();
            this.error = ex;
            DONE.setRelease(this, true);
        }

        @Override
        public void onComplete() {
            findHead();
            DONE.setRelease(this, true);
        }

        Node<T> findHead() {
            long time = executor.now(unit) - timeout;
            Node<T> h = (Node<T>)HEAD.getAcquire(this);
            Node<T> g = h;
            for (;;) {
                Node<T> n = h.getAcquire();
                if (n == null || n.time > time) {
                    break;
                }
                h = n;
            }

            if (g != h) {
                HEAD.compareAndSet(this, g, h);
            }
            return h;
        }

        @Override
        public void replayFused(CachingSubscription<T> cs) {
            int missed = 1;

            for (;;) {

                if (cs.isCancelled()) {
                    cs.node = null;
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);

                if (TAIL.getAcquire(this) != cs.node) {
                    cs.actual.onNext(null);
                }

                if (d) {
                    Throwable ex = error;
                    if (ex == null) {
                        cs.actual.onComplete();
                    } else {
                        cs.actual.onError(ex);
                    }
                    return;
                }

                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public void replayNormal(CachingSubscription<T> cs) {
            int missed = 1;
            FolyamSubscriber<? super T> a = cs.actual;
            Node<T> n = (Node<T>)cs.node;
            if (n == null) {
                n = findHead();
            }
            long e = cs.emitted;

            for (;;) {

                long r = cs.requested();

                for (;;) {

                    if (cs.isCancelled()) {
                        cs.node = null;
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    boolean empty = n == TAIL.getAcquire(this);

                    if (d && empty) {
                        cs.node = null;
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        return;
                    }

                    if (empty || e == r) {
                        break;
                    }

                    Node<T> h = n.getAcquire();

                    a.onNext(h.value);

                    n = h;
                    e++;
                }

                cs.emitted = e;
                cs.node = n;
                missed = cs.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public boolean hasValues() {
            return findHead().getAcquire() != null;
        }

        @Override
        public T[] toArray(T[] array) {
            Node<T> h = findHead();
            Node<T> t = h;
            int s = 0;
            for (;;) {
                Node<T> u = t.getAcquire();
                if (u == null) {
                    break;
                }
                s++;
                t = u;
            }

            if (s > array.length) {
                array = (T[])Array.newInstance(array.getClass().getComponentType(), s);
            }

            for (int i = 0; i < s; i++) {
                Node<T> u = h.get();
                array[i] = u.value;
                h = u;
            }

            if (s < array.length) {
                array[s] = null;
            }

            return array;
        }

        @Override
        public T poll(CachingSubscription<T> cs) {
            Node<T> n = (Node<T>)cs.node;
            if (n == null) {
                n = findHead();
                cs.node = n;
            }
            boolean d = (boolean)DONE.getAcquire(this);
            Node<T> h = n.getAcquire();
            if (h == null) {
                if (d) {
                    cs.node = null;
                }
                return null;
            }
            cs.node = h;
            return h.value;
        }

        @Override
        public boolean isEmpty(CachingSubscription<T> cs) {
            return cs.node == TAIL.getAcquire(this);
        }

        @Override
        public void clear(CachingSubscription<T> cs) {
            cs.node = TAIL.getAcquire(this);
        }

        static final class Node<T> extends AtomicReference<Node<T>> {
            final T value;
            final long time;

            Node(T value, long time) {
                this.value = value;
                this.time = time;
            }
        }
    }


    static final class CachingSubscription<T> extends AtomicInteger implements FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final BufferManager<T> manager;

        final CachingProcessor<T> parent;

        Object node;

        int nodeIndex;

        long emitted;

        boolean outputFused;

        boolean cancelled;
        static final VarHandle CANCELLED;

        long requested;
        static final VarHandle REQUESTED;

        static {
            try {
                REQUESTED = MethodHandles.lookup().findVarHandle(CachingSubscription.class, "requested", long.class);
                CANCELLED = MethodHandles.lookup().findVarHandle(CachingSubscription.class, "cancelled", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        CachingSubscription(FolyamSubscriber<? super T> actual, BufferManager<T> manager, CachingProcessor<T> parent) {
            this.actual = actual;
            this.manager = manager;
            this.parent = parent;
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
            return manager.poll(this);
        }

        @Override
        public boolean isEmpty() {
            return manager.isEmpty(this);
        }

        @Override
        public void clear() {
            manager.clear(this);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            manager.replay(this);
        }

        @Override
        public void cancel() {
            if (CANCELLED.compareAndSet(this, false, true)) {
                parent.remove(this);
                if (getAndIncrement() == 0) {
                    node = null;
                }
            }
        }

        boolean isCancelled() {
            return (boolean)CANCELLED.getAcquire(this);
        }

        long requested() {
            return (long)REQUESTED.getAcquire(this);
        }
    }
}
