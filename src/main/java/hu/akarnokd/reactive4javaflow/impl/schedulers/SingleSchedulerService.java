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

public final class SingleSchedulerService implements SchedulerService, ThreadFactory {

    final String namePrefix;

    final int priority;

    final boolean daemon;

    long index;
    static final VarHandle INDEX;

    ScheduledExecutorService exec = SHUTDOWN;
    static final VarHandle EXEC;

    static final ScheduledExecutorService SHUTDOWN;

    static {
        try {
            EXEC = MethodHandles.lookup().findVarHandle(SingleSchedulerService.class, "exec", ScheduledExecutorService.class);
            INDEX = MethodHandles.lookup().findVarHandle(SingleSchedulerService.class, "index", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
        SHUTDOWN = Executors.newScheduledThreadPool(0);
        SHUTDOWN.shutdown();
    }

    public SingleSchedulerService(String namePrefix, int priority, boolean daemon) {
        this.namePrefix = namePrefix;
        this.priority = priority;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + ((int)INDEX.getAndAdd(this, 1) + 1));
        thread.setPriority(priority);
        thread.setDaemon(daemon);
        return thread;
    }

    @Override
    public AutoDisposable schedule(Runnable task) {
        Objects.requireNonNull(task, "task == null");
        ScheduledExecutorService exec = (ScheduledExecutorService)EXEC.getAcquire(this);
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.submit((Callable<Void>)wt);
            wt.setFuture(f);
            return wt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
        }
        return REJECTED;
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        Objects.requireNonNull(task, "task == null");
        ScheduledExecutorService exec = (ScheduledExecutorService)EXEC.getAcquire(this);
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.schedule((Callable<Void>)wt, delay, unit);
            wt.setFuture(f);
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
        ScheduledExecutorService exec = (ScheduledExecutorService)EXEC.getAcquire(this);
        WorkerTask wt = new WorkerTask(task, null);
        try {
            Future<?> f = exec.scheduleAtFixedRate(wt, initialDelay, period, unit);
            wt.setFuturePeriodic(f);
            return wt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
        }
        return REJECTED;
    }

    @Override
    public Worker worker() {
        return new ScheduledExecutorServiceWorker((ScheduledExecutorService)EXEC.getAcquire(this));
    }

    @Override
    public void start() {
        ScheduledExecutorService b = null;
        for (;;) {
            ScheduledExecutorService a = (ScheduledExecutorService)EXEC.getAcquire(this);
            if (a != SHUTDOWN) {
                if (b != null) {
                    b.shutdown();
                }
                return;
            }
            if (b == null) {
                b = Executors.newSingleThreadScheduledExecutor(this);
                ((ScheduledThreadPoolExecutor)b).setRemoveOnCancelPolicy(true);
            }
            if (EXEC.compareAndSet(this, a, b)) {
                return;
            }
        }
    }

    @Override
    public void shutdown() {
        ((ScheduledExecutorService)EXEC.getAndSet(this, SHUTDOWN)).shutdownNow();
    }
}
