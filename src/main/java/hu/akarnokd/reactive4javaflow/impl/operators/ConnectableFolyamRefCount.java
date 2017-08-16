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
import hu.akarnokd.reactive4javaflow.disposables.SequentialAutoDisposable;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

/**
 * A refCount implementation that allows connecting to the source after the specified
 * number of Subscribers subscribed and allows disconnecting after a specified
 * grace period.
 */
public final class ConnectableFolyamRefCount<T> extends Folyam<T> {

    final ConnectableFolyam<T> source;

    final int n;

    final long timeout;

    final TimeUnit unit;

    final SchedulerService scheduler;

    RefConnection connection;

    public ConnectableFolyamRefCount(ConnectableFolyam<T> source, int n, long timeout, TimeUnit unit,
                            SchedulerService scheduler) {
        this.source = source;
        this.n = n;
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        RefConnection conn;

        boolean connect = false;
        synchronized (this) {
            conn = connection;
            if (conn == null || conn.terminated) {
                conn = new RefConnection(this);
                connection = conn;
            }

            long c = conn.subscriberCount;
            if (c == 0L && conn.timer != null) {
                conn.timer.close();
            }
            conn.subscriberCount = c + 1;
            if (!conn.connected && c + 1 == n) {
                connect = true;
                conn.connected = true;
            }
        }

        source.subscribe(new RefCountSubscriber<T>(s, this, conn));

        if (connect) {
            source.connect(conn);
        }
    }

    void cancel(RefConnection rc) {
        SequentialAutoDisposable sd;
        synchronized (this) {
            if (rc.terminated) {
                return;
            }
            long c = rc.subscriberCount - 1;
            rc.subscriberCount = c;
            if (c != 0L || !rc.connected) {
                return;
            }
            if (timeout == 0L) {
                timeout(rc);
                return;
            }
            sd = new SequentialAutoDisposable();
            rc.timer = sd;
        }

        sd.replace(scheduler.schedule(rc, timeout, unit));
    }

    void terminated(RefConnection rc) {
        synchronized (this) {
            if (!rc.terminated) {
                rc.terminated = true;
                source.reset();
                connection = null;
            }
        }
    }

    void timeout(RefConnection rc) {
        synchronized (this) {
            if (rc.subscriberCount == 0 && rc == connection) {
                DisposableHelper.close(rc);
                source.reset();
                connection = null;
            }
        }
    }

    static final class RefConnection extends AtomicReference<AutoDisposable>
            implements Runnable, Consumer<AutoDisposable> {

        private static final long serialVersionUID = -4552101107598366241L;

        final ConnectableFolyamRefCount<?> parent;

        AutoDisposable timer;

        long subscriberCount;

        boolean connected;

        boolean terminated;

        RefConnection(ConnectableFolyamRefCount<?> parent) {
            this.parent = parent;
        }

        @Override
        public void run() {
            parent.timeout(this);
        }

        @Override
        public void accept(AutoDisposable t) {
            DisposableHelper.replace(this, t);
        }
    }

    static final class RefCountSubscriber<T>
            extends AtomicBoolean implements FolyamSubscriber<T>, Flow.Subscription {

        private static final long serialVersionUID = -7419642935409022375L;

        final FolyamSubscriber<? super T> actual;

        final ConnectableFolyamRefCount<T> parent;

        final RefConnection connection;

        Flow.Subscription upstream;

        RefCountSubscriber(FolyamSubscriber<? super T> actual, ConnectableFolyamRefCount<T> parent, RefConnection connection) {
            this.actual = actual;
            this.parent = parent;
            this.connection = connection;
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
            if (compareAndSet(false, true)) {
                parent.terminated(connection);
            }
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            if (compareAndSet(false, true)) {
                parent.terminated(connection);
            }
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            if (compareAndSet(false, true)) {
                parent.cancel(connection);
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.upstream = s;

            actual.onSubscribe(this);
        }
    }
}
