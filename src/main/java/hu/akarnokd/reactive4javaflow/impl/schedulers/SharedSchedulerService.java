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

import hu.akarnokd.reactive4javaflow.SchedulerService;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.DisposableHelper;
import hu.akarnokd.reactive4javaflow.impl.util.OpenHashSet;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class SharedSchedulerService implements SchedulerService {

    final SchedulerService.Worker worker;

    /**
     * Constructs a SharedSchedulerService and asks for a Worker from the provided other Scheduler.
     * @param other the other SchedulerService instance, not null
     */
    public SharedSchedulerService(SchedulerService other) {
        this.worker = other.worker();
    }

    /**
     * Constructs a SharedSchedulerService and uses the Worker instance provided.
     * @param worker the worker to use, not null
     */
    public SharedSchedulerService(SchedulerService.Worker worker) {
        this.worker = worker;
    }

    @Override
    public void shutdown() {
        worker.close();
    }

    @Override
    public AutoDisposable schedule(Runnable run) {
        return worker.schedule(run);
    }

    @Override
    public AutoDisposable schedule(Runnable run, long delay, TimeUnit unit) {
        return worker.schedule(run, delay, unit);
    }

    @Override
    public AutoDisposable schedulePeriodically(Runnable run, long initialDelay, long period, TimeUnit unit) {
        return worker.schedulePeriodically(run, initialDelay, period, unit);
    }

    @Override
    public long now(TimeUnit unit) {
        return worker.now(unit);
    }

    @Override
    public Worker worker() {
        return new SharedWorker(worker);
    }

    static final class SharedWorker implements SchedulerService.Worker {

        final Worker worker;

        OpenHashSet<AutoDisposable> tasks;

        volatile boolean closed;

        SharedWorker(Worker worker) {
            this.worker = worker;
            this.tasks = new OpenHashSet<>();
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

                Object[] as = set.keys();
                for (Object o : as) {
                    if (o != null) {
                        ((AutoDisposable)o).close();
                    }
                }
            }
        }

        protected boolean add(AutoDisposable d) {
            if (!closed) {
                synchronized (this) {
                    if (!closed) {
                        tasks.add(d);
                        return true;
                    }
                }
            }
            return false;
        }

        public void remove(AutoDisposable d) {
            if (!closed) {
                synchronized (this) {
                    if (!closed) {
                        tasks.remove(d);
                    }
                }
            }
        }

        @Override
        public AutoDisposable schedule(Runnable run, long delay, TimeUnit unit) {
            Objects.requireNonNull(run, "run == null");

            SharedAction sa = new SharedAction(run, this);
            if (add(sa)) {

                AutoDisposable d;
                if (delay <= 0L) {
                    d = worker.schedule(sa);
                } else {
                    d = worker.schedule(sa, delay, unit);
                }
                sa.setFuture(d);

                return sa;
            }
            return REJECTED;
        }

        @Override
        public long now(TimeUnit unit) {
            return worker.now(unit);
        }

        static final class SharedAction
                extends AtomicReference<SharedWorker>
                implements Runnable, AutoDisposable {
            private static final long serialVersionUID = 4949851341419870956L;

            AutoDisposable future;
            static final VarHandle FUTURE;

            final Runnable actual;

            static {
                try {
                    FUTURE = MethodHandles.lookup().findVarHandle(SharedAction.class, "future", AutoDisposable.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            SharedAction(Runnable actual, SharedWorker parent) {
                this.actual = actual;
                this.setRelease(parent);
            }

            @Override
            public void run() {
                try {
                    actual.run();
                } finally {
                    complete();
                }
            }

            void complete() {
                SharedWorker cd = get();
                if (cd != null && compareAndSet(cd, null)) {
                    cd.remove(this);
                }
                for (;;) {
                    AutoDisposable f = (AutoDisposable)FUTURE.getAcquire(this);
                    if (f == DisposableHelper.CLOSED || FUTURE.compareAndSet(this, f, this)) {
                        break;
                    }
                }
            }

            @Override
            public void close() {
                SharedWorker cd = getAndSet(null);
                if (cd != null) {
                    cd.remove(this);
                }
                DisposableHelper.close(this, FUTURE);
            }

            void setFuture(AutoDisposable d) {
                AutoDisposable f = (AutoDisposable)FUTURE.getAcquire(this);
                if (f != this) {
                    if (f == DisposableHelper.CLOSED) {
                        d.close();
                    } else
                    if (!FUTURE.compareAndSet(this, f, d)) {
                        f = (AutoDisposable)FUTURE.getAcquire(this);
                        if (f == DisposableHelper.CLOSED) {
                            d.close();
                        }
                    }
                }
            }
        }
    }
}
