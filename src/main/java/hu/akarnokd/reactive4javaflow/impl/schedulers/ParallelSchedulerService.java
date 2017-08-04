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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.*;

public final class ParallelSchedulerService implements SchedulerService, ThreadFactory {
    final int parallelism;

    final String namePrefix;

    final int priority;

    final boolean daemon;

    long index;
    static final VarHandle INDEX;

    ScheduledExecutorService[] executors;
    static final VarHandle EXECUTORS;

    static final ScheduledExecutorService[] SHUTDOWN = new ScheduledExecutorService[0];

    static final ScheduledExecutorService STOPPED;

    int n;

    static {
        try {
            INDEX = MethodHandles.lookup().findVarHandle(ParallelSchedulerService.class, "index", Long.TYPE);
            EXECUTORS = MethodHandles.lookup().findVarHandle(ParallelSchedulerService.class, "executors", ScheduledExecutorService[].class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
        STOPPED = Executors.newScheduledThreadPool(0);
        STOPPED.shutdown();
    }

    public ParallelSchedulerService(int parallelism, String namePrefix, int priority, boolean daemon) {
        this.parallelism = parallelism;
        this.namePrefix = namePrefix;
        this.priority = priority;
        this.daemon = daemon;
        EXECUTORS.setRelease(this, create());
    }

    ScheduledExecutorService pick() {
        ScheduledExecutorService[] a = (ScheduledExecutorService[])EXECUTORS.getAcquire(this);
        if (a == SHUTDOWN) {
            return STOPPED;
        }
        int idx = n;
        ScheduledExecutorService b = a[idx];
        int j = idx + 1;
        n = j == n ? 0 : j;
        return b;
    }


    @Override
    public AutoDisposable schedule(Runnable task) {
        Objects.requireNonNull(task, "task == null");
        ScheduledExecutorService exec = pick();
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.submit((Callable<Void>)wt);
            wt.setFutureNoCancel(f);
            return wt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
        }
        return REJECTED;
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        Objects.requireNonNull(task, "task == null");
        ScheduledExecutorService exec = pick();
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.schedule((Callable<Void>)wt, delay, unit);
            wt.setFutureNoCancel(f);
            return wt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
        }
        return REJECTED;
    }

    @Override
    public AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        Objects.requireNonNull(task, "task == null");
        if (period <= 0L) {
            return SchedulerService.super.schedulePeriodically(task, initialDelay, period, unit);
        }
        ScheduledExecutorService exec = pick();
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.scheduleAtFixedRate(wt, initialDelay, period, unit);
            wt.setFutureNoCancel(f);
            return wt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
        }
        return REJECTED;
    }

    @Override
    public Worker worker() {
        return new ScheduledExecutorServiceWorker(pick());
    }

    @Override
    public void start() {
        ScheduledExecutorService[] b = null;
        for (;;) {
            ScheduledExecutorService[] a = (ScheduledExecutorService[])EXECUTORS.getAcquire(this);
            if (a != SHUTDOWN) {
                if (b != null) {
                    for (ScheduledExecutorService c : b) {
                        c.shutdown();
                    }
                    return;
                }
            }
            if (b == null) {
                b = create();
            }
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }

    @Override
    public void shutdown() {
        ScheduledExecutorService[] a = (ScheduledExecutorService[])EXECUTORS.getAndSet(this, SHUTDOWN);
        for (ScheduledExecutorService b : a) {
            b.shutdownNow();
        }
    }

    ScheduledExecutorService[] create() {
        int p = parallelism;
        ScheduledExecutorService[] a = new ScheduledExecutorService[p];

        for (int i = 0; i < p; i++) {
            ScheduledExecutorService b = Executors.newScheduledThreadPool(1, this);
            ((ScheduledThreadPoolExecutor)b).setRemoveOnCancelPolicy(true);
            a[i] = b;
        }

        return a;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + ((long)INDEX.getAndAdd(this, 1) + 1));
        thread.setPriority(priority);
        thread.setDaemon(daemon);
        return thread;
    }
}
