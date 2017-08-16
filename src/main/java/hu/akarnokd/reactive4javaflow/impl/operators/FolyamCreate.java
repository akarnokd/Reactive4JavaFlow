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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.DisposableHelper;
import hu.akarnokd.reactive4javaflow.impl.PlainQueue;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

public final class FolyamCreate<T> extends Folyam<T> {

    final CheckedConsumer<? super FolyamEmitter<T>> onSubscribe;

    final BackpressureHandling mode;

    public FolyamCreate(CheckedConsumer<? super FolyamEmitter<T>> onSubscribe, BackpressureHandling mode) {
        this.onSubscribe = onSubscribe;
        this.mode = mode;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractFolyamEmitter<T> emitter;

        switch (mode) {
            case BUFFER:
                emitter = new BufferFolyamEmitter<>(s, FolyamPlugins.defaultBufferSize());
                break;
            case DROP:
                emitter = new DropFolyamEmitter<>(s);
                break;
            case LATEST:
                emitter = new LatestFolyamEmitter<>(s);
                break;
            case ERROR:
                emitter = new ErrorFolyamEmitter<>(s);
                break;
            default:
                emitter = new MissingFolyamEmitter<>(s);
        }

        s.onSubscribe(emitter);
        try {
            onSubscribe.accept(emitter);
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            emitter.onError(ex);
        }
    }

    /*
    public static <T> FolyamEmitter<T> createBufferedEmitter(FolyamSubscriber<? super T> subscriber, int capacityHint) {
        return new BufferFolyamEmitter<>(subscriber, capacityHint);
    }
    */

    static abstract class AbstractFolyamEmitter<T> implements FolyamEmitter<T>, Flow.Subscription {

        AutoCloseable resource;
        static final VarHandle RESOURCE;

        static {
            try {
                RESOURCE = MethodHandles.lookup().findVarHandle(AbstractFolyamEmitter.class, "resource", AutoCloseable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        @Override
        public void setResource(AutoCloseable resource) {
            for (;;) {
                AutoCloseable res = (AutoCloseable) RESOURCE.getAcquire(this);
                if (res == DisposableHelper.CLOSED) {
                    if (resource != null) {
                        try {
                            resource.close();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    }
                    return;
                }
                if (RESOURCE.compareAndSet(this, res, resource)) {
                    if (res != null) {
                        try {
                            res.close();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    }
                    return;
                }
            }
        }

        @Override
        public boolean isCancelled() {
            return RESOURCE.getAcquire(this) == DisposableHelper.CLOSED;
        }

        @Override
        public void cancel() {
            AutoCloseable res = (AutoCloseable) RESOURCE.getAcquire(this);
            if (res != DisposableHelper.CLOSED) {
                res = (AutoCloseable)RESOURCE.getAndSet(this, DisposableHelper.CLOSED);
                if (res != null && res != DisposableHelper.CLOSED) {
                    try {
                        res.close();
                    } catch (Throwable ex) {
                        FolyamPlugins.onError(ex);
                    }
                }
            }
        }

        @Override
        public FolyamEmitter<T> serialized() {
            return new SerializedFolyamEmitter<>(this);
        }

        @Override
        public final void onError(Throwable ex) {
            if (ex == null) {
                ex = new NullPointerException("ex == null");
            }
            if (!tryOnError(ex)) {
                FolyamPlugins.onError(ex);
            }
        }
    }

    static final class SerializedFolyamEmitter<T> implements FolyamEmitter<T> {

        final AbstractFolyamEmitter<T> actual;

        boolean emitting;
        boolean missed;

        List<T> queue;

        Throwable error;
        boolean done;

        SerializedFolyamEmitter(AbstractFolyamEmitter<T> actual) {
            this.actual = actual;
        }

        @Override
        public long requested() {
            return actual.requested();
        }

        @Override
        public FolyamEmitter<T> serialized() {
            return this;
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
                return;
            }
            synchronized (this) {
                if (emitting) {
                    if (!done) {
                        List<T> q = queue;
                        if (q == null) {
                            q = new ArrayList<>();
                            queue = q;
                        }
                        q.add(item);
                        missed = true;
                    }
                    return;
                }
                emitting = true;
            }

            actual.onNext(item);

            for (;;) {
                List<T> q;
                boolean d;
                Throwable ex;
                synchronized (this) {
                    if (!missed) {
                        missed = false;
                        emitting = false;
                        return;
                    }
                    missed = false;
                    q = queue;
                    queue = null;
                    d = done;
                    ex = error;
                }

                for (T v : q) {
                    if (actual.isCancelled()) {
                        return;
                    }
                    actual.onNext(v);
                }

                if (d) {
                    if (ex == null) {
                        actual.onComplete();
                    } else {
                        actual.onError(ex);
                    }
                    return;
                }
            }
        }

        @Override
        public void onComplete() {
            synchronized (this) {
                if (emitting) {
                    done = true;
                    missed = true;
                    return;
                }
                emitting = true;
                done = true;
            }

            actual.onComplete();
        }

        @Override
        public boolean tryOnError(Throwable ex) {
            if (ex == null) {
                ex = new NullPointerException("ex == null");
            }
            synchronized (this) {
                if (emitting) {
                    if (!done && error == null) {
                        error = ex;
                        done = true;
                        missed = true;
                        return true;
                    }
                    return false;
                }
                emitting = true;
                done = true;
            }

            return actual.tryOnError(ex);
        }

        @Override
        public void onError(Throwable ex) {
            if (!tryOnError(ex)) {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public boolean isCancelled() {
            return actual.isCancelled();
        }

        @Override
        public void setResource(AutoCloseable resource) {
            actual.setResource(resource);
        }
    }

    static abstract class AbstractBackpressuredFolyamEmitter<T> extends AbstractFolyamEmitter<T> {

        final FolyamSubscriber<? super T> subscriber;

        long requested;
        static final VarHandle REQUESTED;

        static {
            try {
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractBackpressuredFolyamEmitter.class, "requested", Long.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractBackpressuredFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public final long requested() {
            return (long)REQUESTED.getAcquire(this);
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
        }

        @Override
        public boolean tryOnError(Throwable ex) {
            AutoCloseable res = (AutoCloseable)RESOURCE.getAndSet(this, DisposableHelper.CLOSED);
            if (res != DisposableHelper.CLOSED) {
                if (ex == null) {
                    ex = new NullPointerException("ex == null");
                }
                subscriber.onError(ex);
                if (res != null) {
                    try {
                        res.close();
                    } catch (Throwable exc) {
                        FolyamPlugins.onError(exc);
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public void onComplete() {
            AutoCloseable res = (AutoCloseable)RESOURCE.getAndSet(this, DisposableHelper.CLOSED);
            if (res != DisposableHelper.CLOSED) {
                subscriber.onComplete();
                if (res != null) {
                    try {
                        res.close();
                    } catch (Throwable ex) {
                        FolyamPlugins.onError(ex);
                    }
                }
            }
        }
    }

    static final class MissingFolyamEmitter<T> extends AbstractBackpressuredFolyamEmitter<T> {

        MissingFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
            } else {
                if (RESOURCE.getAcquire(this) != DisposableHelper.CLOSED) {
                    subscriber.onNext(item);
                }
            }
        }

    }

    static final class DropFolyamEmitter<T> extends AbstractBackpressuredFolyamEmitter<T> {

        long emitted;

        DropFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
                return;
            }
            if (!isCancelled()) {
                long r = requested();
                long e = emitted;
                if (r != e) {
                    emitted = e + 1;
                    subscriber.onNext(item);
                }
            }
        }
    }


    static final class ErrorFolyamEmitter<T> extends AbstractBackpressuredFolyamEmitter<T> {

        long emitted;

        ErrorFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
                return;
            }
            if (!isCancelled()) {
                long r = requested();
                long e = emitted;
                if (r != e) {
                    emitted = e + 1;
                    subscriber.onNext(item);
                } else {
                    onError(new IllegalStateException("Flow.Subscriber is not ready to receive items."));
                }
            }
        }
    }

    static abstract class AbstractBufferingFolyamEmitter<T> extends AbstractBackpressuredFolyamEmitter<T> {

        int wip;
        static final VarHandle WIP;

        Throwable error;

        boolean done;
        static final VarHandle DONE;

        AutoCloseable toClose;

        volatile boolean cancelled;

        long emitted;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(AbstractBufferingFolyamEmitter.class, "wip", Integer.TYPE);
                DONE = MethodHandles.lookup().findVarHandle(AbstractBufferingFolyamEmitter.class, "done", Boolean.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractBufferingFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        public final void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
            } else {
                if (!isCancelled()) {
                    offerItem(item);
                    if (isCancelled()) {
                        cleanup();
                    } else {
                        drain();
                    }
                }
            }
        }

        @Override
        public final boolean tryOnError(Throwable ex) {
            AutoCloseable res = (AutoCloseable)RESOURCE.getAcquire(this);
            if (res != DisposableHelper.CLOSED) {
                res = (AutoCloseable)RESOURCE.getAndSet(this, DisposableHelper.CLOSED);
                if (res != DisposableHelper.CLOSED) {
                    toClose = res;
                    if (ex == null) {
                        ex = new NullPointerException("ex == null");
                    }
                    error = ex;
                    DONE.setRelease(this, true);
                    drain();
                    return true;
                }
            }
            return false;
        }

        @Override
        public final void onComplete() {
            AutoCloseable res = (AutoCloseable)RESOURCE.getAcquire(this);
            if (res != DisposableHelper.CLOSED) {
                res = (AutoCloseable)RESOURCE.getAndSet(this, DisposableHelper.CLOSED);
                if (res != DisposableHelper.CLOSED) {
                    toClose = res;
                    DONE.setRelease(this, true);
                    drain();
                }
            }
        }

        @Override
        public final void cancel() {
            cancelled = true;
            super.cancel();
            cleanup();
        }

        abstract void cleanup();

        abstract void offerItem(T item);

        abstract void drain();

        final void closeResource() {
            AutoCloseable res = toClose;
            toClose = null;
            if (res != null) {
                try {
                    res.close();
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            }
        }
    }

    static final class LatestFolyamEmitter<T> extends AbstractBufferingFolyamEmitter<T> {

        T item;
        static final VarHandle ITEM;

        static {
            try {
                ITEM = MethodHandles.lookup().findVarHandle(LatestFolyamEmitter.class, "item", Object.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        LatestFolyamEmitter(FolyamSubscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        void cleanup() {
            ITEM.setRelease(this, null);
        }

        @Override
        void offerItem(T item) {
            ITEM.getAndSet(this, item);
        }

        @Override
        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }
            int missed = 1;
            long e = emitted;

            FolyamSubscriber<? super T> a = subscriber;

            for (;;) {
                long r = requested();

                while (e != r) {
                    if (cancelled) {
                        cleanup();
                        closeResource();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    @SuppressWarnings("unchecked")
                    T v = (T)ITEM.getAndSet(this, null);
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        closeResource();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        cleanup();
                        closeResource();
                        return;
                    }

                    if ((boolean)DONE.getAcquire(this) && ITEM.getAcquire(this) == null) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        closeResource();
                        return;
                    }
                }

                emitted = e;
                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }
        }
    }


    static final class BufferFolyamEmitter<T> extends AbstractBufferingFolyamEmitter<T> implements FusedSubscription<T> {

        final PlainQueue<T> queue;

        boolean outputFused;

        BufferFolyamEmitter(FolyamSubscriber<? super T> subscriber, int capacityHint) {
            super(subscriber);
            this.queue = new SpscLinkedArrayQueue<>(capacityHint);
        }

        @Override
        void cleanup() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    queue.clear();
                } while ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }

        @Override
        void offerItem(T item) {
            queue.offer(item);
        }

        @Override
        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }
            if (outputFused) {
                drainFused();
            } else {
                drainNormal();
            }
        }

        void drainFused() {
            int missed = 1;

            FolyamSubscriber<? super T> a = subscriber;
            PlainQueue<T> q = queue;

            for (;;) {
                if (cancelled) {
                    q.clear();
                    closeResource();
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
                    closeResource();
                    return;
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }

        }

        void drainNormal() {
            int missed = 1;
            long e = emitted;

            FolyamSubscriber<? super T> a = subscriber;
            PlainQueue<T> q = queue;

            for (;;) {
                long r = requested();

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        closeResource();
                        return;
                    }

                    boolean d = (boolean)DONE.getAcquire(this);
                    @SuppressWarnings("unchecked")
                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        closeResource();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        q.clear();
                        closeResource();
                        return;
                    }

                    if ((boolean)DONE.getAcquire(this) && q.isEmpty()) {
                        Throwable ex = error;
                        if (ex == null) {
                            a.onComplete();
                        } else {
                            a.onError(ex);
                        }
                        closeResource();
                        return;
                    }
                }

                emitted = e;
                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }

        }

        @Override
        public void request(long n) {
            super.request(n);
            drain();
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
}
