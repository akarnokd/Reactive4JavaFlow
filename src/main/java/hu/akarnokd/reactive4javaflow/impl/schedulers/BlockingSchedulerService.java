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
import hu.akarnokd.reactive4javaflow.disposables.SequentialAutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.util.OpenHashSet;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * A Scheduler that uses the current thread, in an event-loop and
 * blocking fashion to execute actions.
 * <p>
 * Use the {@link #start()} to start waiting for tasks (from other
 * threads) or {@link #execute(Runnable)} to start with a first action.
 * <pre><code>
 * public static void main(String[] args) {
 *     BlockingScheduler scheduler = new BlockingScheduler();
 *     scheduler.execute(() -&gt; {
 *         // This executes in the blocking event loop.
 *         // Usually the rest of the "main" method should
 *         // be moved here.
 *
 *         someApi.methodCall()
 *           .subscribeOn(Schedulers.io())
 *           .observeOn(scheduler)
 *           .subscribe(v -&gt; { /* on the main thread *&#47; });
 *     });
 * }
 * </code></pre>
 *
 * In the example code above, {@code observeOn(scheduler)} will execute
 * on the main thread of the Java application.
 *
 */
public final class BlockingSchedulerService implements SchedulerService {

    static final Runnable SHUTDOWN = () -> { };

    final ConcurrentLinkedQueue<Runnable> queue;

    final AtomicLong wip;

    final Lock lock;

    final Condition condition;

    final AtomicBoolean running;

    final AtomicBoolean shutdown;

    final SchedulerService timedHelper;

    volatile Thread thread;

    public BlockingSchedulerService() {
        this.queue = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
        this.running = new AtomicBoolean();
        this.shutdown = new AtomicBoolean();
        this.wip = new AtomicLong();
        this.timedHelper = SchedulerServices.single();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method will block until the {@link #shutdown()} is invoked.
     * @see #execute(Runnable)
     */
    @Override
    public void start() {
        execute(() -> { });
    }

    /**
     * Begin executing the blocking event loop with the given initial action
     * (usually contain the rest of the 'main' method).
     * <p>
     * This method will block until the {@link #shutdown()} is invoked.
     * @param action the action to execute
     */
    public void execute(Runnable action) {
        Objects.requireNonNull(action, "action is null");
        if (!running.get() && running.compareAndSet(false, true)) {
            thread = Thread.currentThread();
            queue.offer(action);
            wip.getAndIncrement();
            drainLoop();
        }
    }

    void drainLoop() {
        final AtomicBoolean stop = shutdown;
        final AtomicLong wip = this.wip;

        for (;;) {
            if (stop.get()) {
                cancelAll();
                return;
            }
            do {
                Runnable a = queue.poll();
                if (a == SHUTDOWN) {
                    cancelAll();
                    return;
                }
                try {
                    a.run();
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            } while (wip.decrementAndGet() != 0);

            if (wip.get() == 0 && !stop.get()) {
                lock.lock();
                try {
                    while (wip.get() == 0 && !stop.get()) {
                        condition.await();
                    }
                } catch (InterruptedException ex) {
                    // deliberately ignored
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    void cancelAll() {
        final ConcurrentLinkedQueue<Runnable> q = queue;

        Runnable a;

        while ((a = q.poll()) != null) {
            if (a instanceof AutoDisposable) {
                ((AutoDisposable)a).close();
            }
        }
    }

    @Override
    public AutoDisposable schedule(Runnable run, long delay, TimeUnit unit) {
        Objects.requireNonNull(run, "run is null");
        Objects.requireNonNull(unit, "unit is null");
        if (shutdown.get()) {
            return REJECTED;
        }

        final BlockingDirectTask task = new BlockingDirectTask(run);

        if (delay == 0L) {
            enqueue(task);
            return task;
        }

        SequentialAutoDisposable inner = new SequentialAutoDisposable();
        final SequentialAutoDisposable outer = new SequentialAutoDisposable(inner);

        AutoDisposable d = timedHelper.schedule(new Runnable() {
            @Override
            public void run() {
                outer.replace(task);
                enqueue(task);
            }
        }, delay, unit);

        if (d == REJECTED) {
            return d;
        }

        inner.replace(d);

        return outer;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            enqueue(SHUTDOWN);
        }
    }

    void enqueue(Runnable action) {
        queue.offer(action);
        if (wip.getAndIncrement() == 0L) {
            lock.lock();
            try {
                condition.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public Worker worker() {
        return new BlockingWorker();
    }

    static final int READY = 0;
    static final int RUNNING = 1;
    static final int INTERRUPTING = 2;
    static final int INTERRUPTED = 3;
    static final int FINISHED = 4;
    static final int CANCELLED = 5;

    final class BlockingDirectTask
            extends AtomicInteger
            implements Runnable, AutoDisposable {

        private static final long serialVersionUID = -9165914884456950194L;
        final Runnable task;

        BlockingDirectTask(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                if (compareAndSet(READY, RUNNING)) {
                    try {
                        task.run();
                    } finally {
                        compareAndSet(RUNNING, FINISHED);
                    }
                }
            } finally {
                while (get() == INTERRUPTING) { }

                if (get() == INTERRUPTED) {
                    Thread.interrupted();
                }
            }
        }

        @Override
        public void close() {
            for (;;) {
                int s = get();
                if (s >= INTERRUPTING) {
                    break;
                }

                if (s == READY && compareAndSet(READY, CANCELLED)) {
                    break;
                }
                if (compareAndSet(RUNNING, INTERRUPTING)) {
                    Thread t = thread;
                    if (t != null) {
                        t.interrupt();
                    }
                    set(INTERRUPTED);
                    break;
                }
            }
        }
    }

    final class BlockingWorker implements SchedulerService.Worker {

        volatile boolean closed;

        OpenHashSet<AutoDisposable> tasks;

        BlockingWorker() {
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
            Objects.requireNonNull(run, "run is null");
            Objects.requireNonNull(unit, "unit is null");

            if (shutdown.get() || closed) {
                return REJECTED;
            }

            final BlockingTask task = new BlockingTask(run);
            if (add(task)) {

                if (delay == 0L) {
                    enqueue(task);
                    return task;
                }

                SequentialAutoDisposable inner = new SequentialAutoDisposable();
                final SequentialAutoDisposable outer = new SequentialAutoDisposable(inner);

                AutoDisposable d = timedHelper.schedule(new Runnable() {
                    @Override
                    public void run() {
                        outer.replace(task);
                        enqueue(task);
                    }
                }, delay, unit);

                if (d == REJECTED) {
                    return d;
                }

                inner.replace(d);

                return outer;
            }
            return REJECTED;
        }

        final class BlockingTask
                extends AtomicInteger
                implements Runnable, AutoDisposable {

            private static final long serialVersionUID = -9165914884456950194L;

            final Runnable task;

            BlockingTask(Runnable task) {
                this.task = task;
            }

            @Override
            public void run() {
                try {
                    if (compareAndSet(READY, RUNNING)) {
                        try {
                            task.run();
                        } finally {
                            compareAndSet(RUNNING, FINISHED);
                            remove(this);
                        }
                    }
                } finally {
                    while (get() == INTERRUPTING) { }

                    if (get() == INTERRUPTED) {
                        Thread.interrupted();
                    }
                }
            }

            @Override
            public void close() {
                for (;;) {
                    int s = get();
                    if (s >= INTERRUPTING) {
                        return;
                    }

                    if (s == READY && compareAndSet(READY, CANCELLED)) {
                        break;
                    }
                    if (compareAndSet(RUNNING, INTERRUPTING)) {
                        Thread t = thread;
                        if (t != null) {
                            t.interrupt();
                        }
                        set(INTERRUPTED);
                        break;
                    }
                }
                remove(this);
            }
        }
    }
}
