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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.OpenHashSet;

import java.lang.invoke.*;
import java.lang.ref.Cleaner;
import java.util.concurrent.*;

public final class IOSchedulerService implements SchedulerService, ThreadFactory {

    static final int CLEAR_PERIOD_SECONDS = 60;

    final String name;

    final int priority;

    final boolean daemon;

    IOPools current;
    static final VarHandle CURRENT = VH.find(MethodHandles.lookup(), IOSchedulerService.class, "current", IOPools.class);

    long index;
    static final VarHandle INDEX = VH.find(MethodHandles.lookup(), IOSchedulerService.class, "index", Long.TYPE);

    static final ScheduledExecutorService STOPPED;

    static {
        STOPPED = Executors.newScheduledThreadPool(0);
        STOPPED.shutdown();
    }

    public IOSchedulerService(String name, int priority, boolean daemon) {
        this.name = name;
        this.priority = priority;
        this.daemon = daemon;
        start();
    }

    @Override
    public void start() {
        IOPools p = null;
        for (;;) {
            if (CURRENT.getAcquire(this) != null) {
                if (p != null) {
                    p.shutdown();
                }
                break;
            }
            if (p == null) {
                p = new IOPools(this, name);
            }
            if (CURRENT.compareAndSet(this, null, p)) {
                p.start();
                break;
            }
        }
    }

    @Override
    public void shutdown() {
        IOPools p = (IOPools)CURRENT.getAndSet(this, null);
        if (p != null) {
            p.shutdown();
        }
    }

    @Override
    public Worker worker() {
        ScheduledExecutorService exec;
        IOPools p = (IOPools)CURRENT.getAcquire(this);

        if (p == null) {
            exec = STOPPED;
        } else {
            exec = p.pick();
        }

        return new IOWorker(exec, p);
    }

    long nextIndex() {
        return (long)INDEX.getAndAdd(this, 1) + 1L;
    }

    /**
     * Test support: clear and stop all queued executors immediately.
     */
    public void clear() {
        IOPools p = (IOPools)CURRENT.getAcquire(this);
        if (p != null) {
            p.runNow(Long.MAX_VALUE);
        }
    }

    /**
     * Test support: returns the number of currently pooled executors.
     * @return the number of currently pooled executors
     */
    public int size() {
        IOPools p = (IOPools)CURRENT.getAcquire(this);

        return p != null ? p.size() : 0;
    }

    static final class IOPools implements Runnable {

        final IOSchedulerService parent;

        final ScheduledExecutorService cleanupWorker;

        final String name;

        final ConcurrentLinkedQueue<PoolItem> queue;

        OpenHashSet<ScheduledExecutorService> all;

        volatile boolean shutdown;

        IOPools(IOSchedulerService parent, String name) {
            this.parent = parent;
            this.cleanupWorker = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, name + ".Evictor");
                t.setDaemon(true);
                return t;
            });
            this.name = name;
            this.all = new OpenHashSet<>();
            this.queue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            runNow(System.currentTimeMillis());
        }

        synchronized int size() {
            return all != null ? all.size() : 0;
        }

        void runNow(long time) {
            ConcurrentLinkedQueue<PoolItem> q = this.queue;
            for (;;) {
                PoolItem pi = q.peek();
                if (pi == null || pi.timeout > time) {
                    break;
                }
                if (q.remove(pi)) {
                    pi.executor.shutdownNow();
                    synchronized (this) {
                        if (all != null) {
                            all.remove(pi.executor);
                        }
                    }
                }
            }
        }


        void start() {
            try {
                cleanupWorker.scheduleAtFixedRate(this, CLEAR_PERIOD_SECONDS, CLEAR_PERIOD_SECONDS, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ex) {
                FolyamPlugins.onError(ex);
            }
        }

        void shutdown() {
            shutdown = true;
            cleanupWorker.shutdownNow();
            OpenHashSet<ScheduledExecutorService> set;
            synchronized (this) {
                set = all;
                all = null;
            }

            if (set != null) {
                Object[] arr = set.keys();
                for (Object o : arr) {
                    if (o != null) {
                        ((ScheduledExecutorService)o).shutdownNow();
                    }
                }
            }
            queue.clear();
        }

        ScheduledExecutorService pick() {
            PoolItem pi = queue.poll();
            if (pi != null) {
                return pi.executor;
            }
            ScheduledExecutorService exec = Executors.newScheduledThreadPool(1, parent);
            ((ScheduledThreadPoolExecutor)exec).setRemoveOnCancelPolicy(true);
            synchronized (this) {
                OpenHashSet<ScheduledExecutorService> set = all;
                if (set != null) {
                    set.add(exec);
                    return exec;
                }
            }
            exec.shutdown();
            return STOPPED;
        }

        void putBack(ScheduledExecutorService exec) {
            if (exec != STOPPED) {
                long due = System.currentTimeMillis() + 1000L * CLEAR_PERIOD_SECONDS;
                queue.offer(new PoolItem(exec, due));
                if (shutdown) {
                    queue.clear();
                }
            }
        }

        static final class PoolItem {

            final ScheduledExecutorService executor;

            final long timeout;

            PoolItem(ScheduledExecutorService executor, long timeout) {
                this.executor = executor;
                this.timeout = timeout;
            }
        }
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, name + "-" + nextIndex());
        thread.setPriority(priority);
        thread.setDaemon(daemon);
        return thread;
    }

    static final class IOWorker extends ScheduledExecutorServiceWorker {

        final IOPools pools;

        final Cleaner.Cleanable cleanable;

        boolean once;
        static final VarHandle ONCE = VH.find(MethodHandles.lookup(), IOWorker.class, "once", boolean.class);

        public IOWorker(ScheduledExecutorService exec, IOPools pools) {
            super(exec);
            this.pools = pools;
            this.cleanable = CleanerHelper.register(this, new IOWorkerCleaner(exec, pools));
        }

        @Override
        public void close() {
            if (ONCE.compareAndSet(this, false, true)) {
                super.close();
                cleanable.clean();
            }
        }

        static final class IOWorkerCleaner implements Runnable {

            final ScheduledExecutorService exec;

            final IOPools pools;

            IOWorkerCleaner(ScheduledExecutorService exec, IOPools pools) {
                this.exec = exec;
                this.pools = pools;
            }

            @Override
            public void run() {
                if (pools != null) {
                    pools.putBack(exec);
                }
            }
        }
    }
}
