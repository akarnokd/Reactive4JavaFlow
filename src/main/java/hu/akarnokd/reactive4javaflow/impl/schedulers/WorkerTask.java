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

package hu.akarnokd.reactive4javaflow.impl.schedulers;

import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.VH;

import java.lang.invoke.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public final class WorkerTask implements Callable<Void>, Runnable, AutoDisposable {

    final Runnable run;

    Consumer<? super WorkerTask> worker;
    static final VarHandle WORKER = VH.find(MethodHandles.lookup(), WorkerTask.class, "worker", Consumer.class);

    Future<?> future;
    static final VarHandle FUTURE = VH.find(MethodHandles.lookup(), WorkerTask.class, "future", Future.class);

    Thread runner;

    static final Future<Void> DONE = new FutureTask<>(() -> null);

    static final Future<Void> CLOSED = new FutureTask<>(() -> null);

    public WorkerTask(Runnable run, Consumer<? super WorkerTask> worker) {
        this.run = run;
        WORKER.setRelease(this, worker);
    }

    @Override
    public void close() {
        Future<?> f = (Future<?>)FUTURE.getAcquire(this);
        if (f != DONE && f != CLOSED) {
            if (FUTURE.compareAndSet(this, f, CLOSED)) {
                if (f != null) {
                    f.cancel(runner != Thread.currentThread());
                }
            }
        }
        Consumer<? super WorkerTask> w = (Consumer<? super WorkerTask>)WORKER.getAndSet(this, null);
        if (w != null) {
            w.accept(this);
        }
    }

    public void setFutureNoCancel(Future<?> future) {
        Future<?> f = (Future<?>)FUTURE.getAcquire(this);
        if (f != DONE) {
            FUTURE.compareAndSet(this, f, future);
        }
    }

    public void setFutureCanCancel(Future<?> future) {
        Future<?> f = (Future<?>)FUTURE.getAcquire(this);
        if (f != DONE && f != CLOSED) {
            if (FUTURE.compareAndSet(this, f, future)) {
                return;
            }
            f = (Future<?>)FUTURE.getAcquire(this);
            if (f == DONE) {
                return;
            }
        }
        f.cancel(runner == Thread.currentThread());
    }

    @Override
    public void run() {
        try {
            runner = Thread.currentThread();
            try {
                run.run();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
                Future<?> f = (Future<?>)FUTURE.getAndSet(this, CLOSED);
                if (f != null) {
                    f.cancel(false);
                }
                Consumer<? super WorkerTask> w = (Consumer<? super WorkerTask>) WORKER.getAcquire(this);
                if (w != null) {
                    WORKER.setRelease(this, null);
                    w.accept(this);
                }
            }
        } finally {
            runner = null;
        }

    }

    @Override
    public Void call() throws Exception {
        try {
            runner = Thread.currentThread();
            try {
                run.run();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
        } finally {
            runner = null;
            FUTURE.setRelease(this, DONE);
            Consumer<? super WorkerTask> w = (Consumer<? super WorkerTask>) WORKER.getAcquire(this);
            if (w != null) {
                WORKER.setRelease(this, null);
                w.accept(this);
            }
        }
        return null;
    }
}
