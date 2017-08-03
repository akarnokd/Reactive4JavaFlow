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
import hu.akarnokd.reactive4javaflow.impl.util.OpenHashSet;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class ScheduledExecutorServiceWorker implements SchedulerService.Worker, Consumer<AutoDisposable> {

    final ScheduledExecutorService exec;

    OpenHashSet<AutoDisposable> tasks;

    volatile boolean closed;

    public ScheduledExecutorServiceWorker(ScheduledExecutorService exec) {
        this.exec = exec;
        this.tasks = new OpenHashSet<>();
    }

    @Override
    public AutoDisposable schedule(Runnable task) {
        Objects.requireNonNull(task, "task == null");
        WorkerTask wt = new WorkerTask(task, this);
        if (add(wt)) {
            Future<?> f;
            try {
                f = exec.submit((Callable<Void>)wt);
                wt.setFuture(f);
                return wt;
            } catch (RejectedExecutionException ex) {
                accept(wt);
                FolyamPlugins.onError(ex);
            }
        }
        return SchedulerService.REJECTED;
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        Objects.requireNonNull(task, "task == null");
        WorkerTask wt = new WorkerTask(task, this);
        if (add(wt)) {
            Future<?> f;
            try {
                f = exec.schedule((Callable<Void>)wt, delay, unit);
                wt.setFuture(f);
                return wt;
            } catch (RejectedExecutionException ex) {
                accept(wt);
                FolyamPlugins.onError(ex);
            }
        }
        return SchedulerService.REJECTED;
    }

    @Override
    public AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        Objects.requireNonNull(task, "task == null");
        if (period <= 0L) {
            return SchedulerService.Worker.super.schedulePeriodically(task, initialDelay, period, unit);
        }
        WorkerTask wt = new WorkerTask(task, this);
        if (add(wt)) {
            Future<?> f;
            try {
                f = exec.scheduleAtFixedRate(wt, initialDelay, period, unit);
                wt.setFuturePeriodic(f);
                return wt;
            } catch (RejectedExecutionException ex) {
                accept(wt);
                FolyamPlugins.onError(ex);
            }
        }
        return SchedulerService.REJECTED;
    }

    boolean add(AutoDisposable d) {
        if (!closed) {
            synchronized (this) {
                if (closed) {
                    return false;
                }
                tasks.add(d);
            }
        }
        return true;
    }

    @Override
    public void accept(AutoDisposable d) {
        if (!closed) {
            synchronized (this) {
                if (!closed) {
                    tasks.remove(d);
                }
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            OpenHashSet<AutoDisposable> set;
            synchronized (this) {
                if (closed) {
                    return;
                }
                set = tasks;
                tasks = null;
                closed = true;
            }

            Object[] o = set.keys();
            for (Object e : o) {
                ((AutoDisposable)e).close();
            }
        }
    }
}
