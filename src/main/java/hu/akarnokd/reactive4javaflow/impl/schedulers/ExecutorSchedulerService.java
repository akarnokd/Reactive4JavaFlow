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

import java.lang.invoke.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class ExecutorSchedulerService implements SchedulerService {

    final Executor executor;

    final boolean trampoline;

    static ScheduledExecutorService timedHelper;
    static final VarHandle TIMED_HELPER;

    static final ScheduledExecutorService SHUTDOWN;

    static final Future<?> CANCELLED;

    static {
        try {
            TIMED_HELPER = MethodHandles.lookup().findStaticVarHandle(ExecutorSchedulerService.class, "timedHelper", ScheduledExecutorService.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
        SHUTDOWN = Executors.newScheduledThreadPool(0);
        SHUTDOWN.shutdown();
        CANCELLED = new FutureTask<>(() -> null);

        startTimedHelpers();
    }

    public static void startTimedHelpers() {
        ScheduledExecutorService b = null;
        for (;;) {
            ScheduledExecutorService a = (ScheduledExecutorService)TIMED_HELPER.getAcquire();
            if (a != SHUTDOWN) {
                if (b != null) {
                    b.shutdown();
                }
                return;
            }
            if (b == null) {
                b = Executors.newScheduledThreadPool(1, r -> {
                    Thread t = new Thread(r, "Reactive4JavaFlow.ExecutorTimedHelper");
                    t.setDaemon(true);
                    return t;
                });
                ((ScheduledThreadPoolExecutor)b).setRemoveOnCancelPolicy(true);
            }
            if (TIMED_HELPER.compareAndSet(a, b)) {
                return;
            }
        }
    }

    public static void shutdownTimedHelpers() {
        ScheduledExecutorService exec = ((ScheduledExecutorService)TIMED_HELPER.getAndSet(SHUTDOWN));
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    public ExecutorSchedulerService(Executor executor, boolean trampoline) {
        this.executor = executor;
        this.trampoline = trampoline;
    }

    @Override
    public AutoDisposable schedule(Runnable task) {
        if (executor instanceof ExecutorService) {
            ExecutorService exec = (ExecutorService) this.executor;
            try {
                Future<?> f = exec.submit(task);
                return () -> f.cancel(true);
            } catch (RejectedExecutionException ex) {
                FolyamPlugins.onError(ex);
                return REJECTED;
            }
        }
        ExecutorDirectTask dt = new ExecutorDirectTask(task);
        try {
            executor.execute(dt);
            return dt;
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
            return REJECTED;
        }
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        if (executor instanceof ScheduledExecutorService) {
            ScheduledExecutorService exec = (ScheduledExecutorService) this.executor;
            try {
                Future<?> f = exec.schedule(task, delay, unit);
                return () -> f.cancel(true);
            } catch (RejectedExecutionException ex) {
                FolyamPlugins.onError(ex);
                return REJECTED;
            }
        }
        if (executor instanceof ExecutorService) {
            ExecutorService exec = (ExecutorService) this.executor;

            DoubleFuture df = new DoubleFuture();

            try {
                Future<?> f = timedHelper.schedule(() -> {
                    df.setNext(exec.submit(task));
                    return null;
                }, delay, unit);

                df.setFirst(f);
            } catch (RejectedExecutionException ex) {
                FolyamPlugins.onError(ex);
                return REJECTED;
            }

            return df;
        }
        ExecutorDirectTimedTask dt = new ExecutorDirectTimedTask(task);
        try {
            Future<?> f = timedHelper.schedule(() -> {
                executor.execute(dt);
                return null;
            }, delay, unit);
            dt.setFirst(f);
        } catch (RejectedExecutionException ex) {
            FolyamPlugins.onError(ex);
            return REJECTED;
        }

        return dt;
    }

    @Override
    public AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        if (executor instanceof ScheduledExecutorService) {
            ScheduledExecutorService exec = (ScheduledExecutorService) this.executor;
            try {
                Future<?> f = exec.scheduleAtFixedRate(task, initialDelay, period, unit);
                return () -> f.cancel(true);
            } catch (RejectedExecutionException ex) {
                return REJECTED;
            }
        }
        return SchedulerService.super.schedulePeriodically(task, initialDelay, period, unit);
    }

    @Override
    public Worker worker() {
        if (trampoline) {
            return new ExecutorTrampolinedWorker(executor);
        }
        return new ExecutorPlainWorker(executor);
    }

    static final class ExecutorDirectTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

        ExecutorDirectTask(Runnable run) {
            setRelease(run);
        }

        @Override
        public void close() {
            setRelease(null);
        }

        @Override
        public void run() {
            Runnable r = getAcquire();
            if (r != null) {
                setRelease(null);
                try {
                    r.run();
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            }
        }
    }

    static final class DoubleFuture implements AutoDisposable {
        Future<?> first;
        static final VarHandle FIRST;

        Future<?> next;
        static final VarHandle NEXT;

        static {
            try {
                FIRST = MethodHandles.lookup().findVarHandle(DoubleFuture.class, "first", Future.class);
                NEXT = MethodHandles.lookup().findVarHandle(DoubleFuture.class, "next", Future.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        @Override
        public void close() {
            Future<?> f = (Future<?>)FIRST.getAndSet(this, CANCELLED);
            if (f != null) {
                f.cancel(true);
            }
            f = (Future<?>)NEXT.getAndSet(this, CANCELLED);
            if (f != null) {
                f.cancel(true);
            }
        }

        void setFirst(Future<?> f) {
            if (!FIRST.compareAndSet(this, null, f)) {
                f.cancel(true);
            }
        }

        void setNext(Future<?> f) {
            Future<?> a = (Future<?>)NEXT.getAcquire(this);
            if (a == CANCELLED || !NEXT.compareAndSet(this, a, f)) {
                f.cancel(true);
            }
        }
    }

    static final class ExecutorDirectTimedTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {
        Future<?> first;
        static final VarHandle FIRST;

        ExecutorDirectTimedTask(Runnable run) {
            setRelease(run);
        }

        static {
            try {
                FIRST = MethodHandles.lookup().findVarHandle(ExecutorDirectTimedTask.class, "first", Future.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        @Override
        public void close() {
            setRelease(null);
            Future<?> f = (Future<?>)FIRST.getAndSet(this, CANCELLED);
            if (f != null) {
                f.cancel(true);
            }
        }

        @Override
        public void run() {
            Runnable r = getAcquire();
            if (r != null) {
                setRelease(null);
                try {
                    r.run();
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            }
        }

        void setFirst(Future<?> f) {
            if (!FIRST.compareAndSet(this, null, f)) {
                f.cancel(true);
            }
        }
    }

    static final class ExecutorPlainWorker implements SchedulerService.Worker, Consumer<AutoDisposable> {

        final Executor executor;

        OpenHashSet<AutoDisposable> tasks;

        volatile boolean closed;

        ExecutorPlainWorker(Executor executor) {
            this.executor = executor;
            this.tasks = new OpenHashSet<>();
        }

        boolean add(AutoDisposable d) {
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
        public AutoDisposable schedule(Runnable task) {
            if (executor instanceof ExecutorService) {
                ExecutorService exec = (ExecutorService) this.executor;

                WorkerTask wt = new WorkerTask(task, this);
                if (add(wt)) {
                    try {
                        Future<?> f = exec.submit((Callable<Void>)wt);
                        wt.setFutureCanCancel(f);
                        return wt;
                    } catch (RejectedExecutionException ex) {
                        FolyamPlugins.onError(ex);
                        accept(wt);
                    }
                }
            } else {
                ExecutorWorkerTask wt = new ExecutorWorkerTask(task, this);
                if (add(wt)) {
                    try {
                        executor.execute(wt);
                        return wt;
                    } catch (RejectedExecutionException ex) {
                        FolyamPlugins.onError(ex);
                        accept(wt);
                    }
                }
            }
            return REJECTED;
        }

        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (executor instanceof ExecutorService) {
                ExecutorService exec = (ExecutorService) this.executor;
                ExecutorWorkerTimedTaskServiced wtt = new ExecutorWorkerTimedTaskServiced(task, this);
                try {
                    Future<?> f = timedHelper.schedule(() -> {
                        wtt.setNext(exec.submit(wtt));
                        return null;
                    }, delay, unit);
                    wtt.setFirst(f);
                    return wtt;
                } catch (RejectedExecutionException ex) {
                    FolyamPlugins.onError(ex);
                    accept(wtt);
                }

            } else {
                ExecutorWorkerTimedTask wtt = new ExecutorWorkerTimedTask(task, this);
                if (add(wtt)) {
                    try {
                        Future<?> f = timedHelper.schedule(() -> {
                            executor.execute(wtt);
                            return null;
                        }, delay, unit);
                        wtt.setFirst(f);
                        return wtt;
                    } catch (RejectedExecutionException ex) {
                        FolyamPlugins.onError(ex);
                        accept(wtt);
                    }
                }
            }
            return REJECTED;
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
                    if (e != null) {
                        ((AutoDisposable) e).close();
                    }
                }
            }
        }

        static final class ExecutorWorkerTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

            Consumer<AutoDisposable> worker;
            static final VarHandle WORKER;

            static {
                try {
                    WORKER = MethodHandles.lookup().findVarHandle(ExecutorWorkerTask.class, "worker", Consumer.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            ExecutorWorkerTask(Runnable run, Consumer<AutoDisposable> worker) {
                this.worker = worker;
                setRelease(run);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void close() {
                setRelease(null);
                Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAndSet(this, null);
                if (w != null) {
                    w.accept(this);
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public void run() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        try {
                            r.run();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    } finally {
                        Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAcquire(this);
                        if (w != null) {
                            WORKER.setRelease(this, null);
                            w.accept(this);
                        }
                    }
                }
            }
        }

        static final class ExecutorWorkerTimedTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

            Consumer<AutoDisposable> worker;
            static final VarHandle WORKER;

            Future<?> first;
            static final VarHandle FIRST;

            static {
                try {
                    FIRST = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTask.class, "first", Future.class);
                    WORKER = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTask.class, "worker", Consumer.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            ExecutorWorkerTimedTask(Runnable run, Consumer<AutoDisposable> worker) {
                this.worker = worker;
                setRelease(run);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void close() {
                setRelease(null);
                Future<?> f = (Future<?>)FIRST.getAndSet(this, CANCELLED);
                if (f != null) {
                    f.cancel(true);
                }
                Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAndSet(this, null);
                if (w != null) {
                    w.accept(this);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        try {
                            r.run();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    } finally {
                        Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAcquire(this);
                        if (w != null) {
                            WORKER.setRelease(this, null);
                            w.accept(this);
                        }
                    }
                }
            }

            void setFirst(Future<?> f) {
                if (!FIRST.compareAndSet(this, null, f)) {
                    f.cancel(true);
                }
            }
        }

        static final class ExecutorWorkerTimedTaskServiced extends AtomicReference<Runnable> implements Callable<Void>, AutoDisposable {

            Consumer<AutoDisposable> worker;
            static final VarHandle WORKER;

            Future<?> first;
            static final VarHandle FIRST;

            Future<?> next;
            static final VarHandle NEXT;

            static {
                try {
                    FIRST = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTaskServiced.class, "first", Future.class);
                    NEXT = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTaskServiced.class, "next", Future.class);
                    WORKER = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTaskServiced.class, "worker", Consumer.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            ExecutorWorkerTimedTaskServiced(Runnable run, Consumer<AutoDisposable> worker) {
                this.worker = worker;
                setRelease(run);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void close() {
                setRelease(null);
                Future<?> f = (Future<?>)FIRST.getAndSet(this, CANCELLED);
                if (f != null) {
                    f.cancel(true);
                }
                f = (Future<?>)NEXT.getAndSet(this, CANCELLED);
                if (f != null) {
                    f.cancel(true);
                }
                Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAndSet(this, null);
                if (w != null) {
                    w.accept(this);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public Void call() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        try {
                            r.run();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    } finally {
                        Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAcquire(this);
                        if (w != null) {
                            WORKER.setRelease(this, null);
                            w.accept(this);
                        }
                    }
                }
                return null;
            }

            void setFirst(Future<?> f) {
                if (!FIRST.compareAndSet(this, null, f)) {
                    f.cancel(true);
                }
            }

            void setNext(Future<?> f) {
                if (!NEXT.compareAndSet(this, null, f)) {
                    f.cancel(true);
                }
            }
        }
    }

    static final class ExecutorTrampolinedWorker implements SchedulerService.Worker, Runnable, Callable<Void>, Consumer<AutoDisposable> {

        final Executor executor;

        long wip;
        static final VarHandle WIP;

        final ConcurrentLinkedQueue<Runnable> queue;

        OpenHashSet<AutoDisposable> tasks;

        volatile boolean closed;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(ExecutorTrampolinedWorker.class, "wip", Long.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        ExecutorTrampolinedWorker(Executor executor) {
            this.executor = executor;
            this.queue = new ConcurrentLinkedQueue<>();
            this.tasks = new OpenHashSet<>();
        }

        @Override
        public AutoDisposable schedule(Runnable task) {
            ExecutorWorkerTask wt = new ExecutorWorkerTask(task, this);
            if (add(wt)) {
                queue.offer(wt);
                if (!closed) {
                    try {
                        schedule();
                        return wt;
                    } catch (RejectedExecutionException ex) {
                        FolyamPlugins.onError(ex);
                        accept(wt);
                    }
                } else {
                    queue.clear();
                }
            }
            return REJECTED;
        }

        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            ExecutorWorkerTimedTask wt = new ExecutorWorkerTimedTask(task, this);
            if (add(wt)) {
                try {
                    Future<?> f = timedHelper.submit(() -> {
                        queue.offer(wt);
                        if (!closed) {
                            try {
                                schedule();
                            } catch (RejectedExecutionException ex) {
                                FolyamPlugins.onError(ex);
                                accept(wt);
                            }
                        } else {
                            queue.clear();
                        }
                        return null;
                    });
                    wt.setFirst(f);
                    return wt;
                } catch (RejectedExecutionException ex) {
                    FolyamPlugins.onError(ex);
                }
                accept(wt);
            }
            return REJECTED;
        }

        @Override
        public void accept(AutoDisposable d) {
            queue.remove(d);
            if (!closed) {
                synchronized (this) {
                    if (!closed) {
                        tasks.remove(d);
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

        void schedule() {
            if ((long)WIP.getAndAdd(this, 1) == 0) {
                if (executor instanceof ExecutorService) {
                    ((ExecutorService) executor).submit((Callable<Void>)this);
                } else {
                    executor.execute(this);
                }
            }
        }

        @Override
        public void run() {
            long missed = 1;
            Queue<Runnable> q = queue;

            for (;;) {

                for (;;) {
                    if (closed) {
                        q.clear();
                        break;
                    } else {
                        Runnable run = q.poll();
                        if (run == null) {
                            break;
                        }

                        run.run();
                    }
                }

                missed = (long)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0L) {
                    break;
                }
            }
        }

        @Override
        public Void call() throws Exception {
            run();
            return null;
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

                queue.clear();
                Object[] o = set.keys();
                for (Object e : o) {
                    if (e != null) {
                        ((AutoDisposable) e).close();
                    }
                }
            }
        }

        static final class ExecutorWorkerTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

            Consumer<AutoDisposable> worker;
            static final VarHandle WORKER;

            static {
                try {
                    WORKER = MethodHandles.lookup().findVarHandle(ExecutorWorkerTask.class, "worker", Consumer.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            ExecutorWorkerTask(Runnable run, Consumer<AutoDisposable> worker) {
                this.worker = worker;
                setRelease(run);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void close() {
                setRelease(null);
                Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAndSet(this, null);
                if (w != null) {
                    w.accept(this);
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public void run() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        try {
                            r.run();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    } finally {
                        Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAcquire(this);
                        if (w != null) {
                            WORKER.setRelease(this, null);
                            w.accept(this);
                        }
                    }
                }
            }
        }
        static final class ExecutorWorkerTimedTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

            Consumer<AutoDisposable> worker;
            static final VarHandle WORKER;

            Future<?> first;
            static final VarHandle FIRST;

            static {
                try {
                    FIRST = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTask.class, "first", Future.class);
                    WORKER = MethodHandles.lookup().findVarHandle(ExecutorWorkerTimedTask.class, "worker", Consumer.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            ExecutorWorkerTimedTask(Runnable run, Consumer<AutoDisposable> worker) {
                this.worker = worker;
                setRelease(run);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void close() {
                setRelease(null);
                Future<?> f = (Future<?>)FIRST.getAndSet(this, CANCELLED);
                if (f != null) {
                    f.cancel(true);
                }
                Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAndSet(this, null);
                if (w != null) {
                    w.accept(this);
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public void run() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        try {
                            r.run();
                        } catch (Throwable ex) {
                            FolyamPlugins.onError(ex);
                        }
                    } finally {
                        Consumer<AutoDisposable> w = (Consumer<AutoDisposable>)WORKER.getAcquire(this);
                        if (w != null) {
                            WORKER.setRelease(this, null);
                            w.accept(this);
                        }
                    }
                }
            }

            void setFirst(Future<?> f) {
                if (!FIRST.compareAndSet(this, null, f)) {
                    f.cancel(true);
                }
            }
        }
    }
}
