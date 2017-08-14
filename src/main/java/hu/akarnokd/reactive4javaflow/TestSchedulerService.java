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

package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class TestSchedulerService implements SchedulerService {

    final PriorityBlockingQueue<TestTask> queue;

    long timeNanos;

    long index;
    static final VarHandle INDEX;

    long workers;
    static final VarHandle WORKERS;

    static {
        try {
            INDEX = MethodHandles.lookup().findVarHandle(TestSchedulerService.class, "index", Long.TYPE);
            WORKERS = MethodHandles.lookup().findVarHandle(TestSchedulerService.class, "workers", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public TestSchedulerService() {
        queue = new PriorityBlockingQueue<>();
    }

    long nextIndex() {
        return (long)INDEX.getAndAdd(this, 1);
    }

    @Override
    public AutoDisposable schedule(Runnable task) {
        TestTask tt = new TestTask(task, timeNanos, nextIndex(), null);
        queue.offer(tt);
        return tt;
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        TestTask tt = new TestTask(task, timeNanos + unit.toNanos(delay), nextIndex(), null);
        queue.offer(tt);
        return tt;
    }

    @Override
    public Worker worker() {
        WORKERS.getAndAdd(this, 1);
        return new TestWorker();
    }

    @Override
    public long now(TimeUnit unit) {
        return unit.convert(timeNanos, TimeUnit.NANOSECONDS);
    }

    public void advanceTimeBy(long time, TimeUnit unit) {
        drainUntil(timeNanos + unit.toNanos(time));
    }

    /**
     * Returns the number of active workers of this TestSchedulerService.
     * @return the number of active workers
     */
    public long activeWorkers() {
        return (long)WORKERS.getAcquire(this);
    }

    void decrementWorker() {
        WORKERS.getAndAdd(this, -1);
    }

    void drainUntil(long upToNanos) {
        long now = timeNanos;
        PriorityBlockingQueue<TestTask> q = queue;
        for (;;) {
            TestTask tt = q.peek();
            if (tt == null || tt.dueNanos > upToNanos) {
                break;
            }
            q.poll();
            timeNanos = tt.dueNanos;
            tt.run();
        }
        timeNanos = upToNanos;
    }

    void clear(Object tag) {
        queue.removeIf(tt -> tt.parent == tag);
    }

    final class TestWorker extends AtomicBoolean implements SchedulerService.Worker {

        @Override
        public AutoDisposable schedule(Runnable task) {
            if (!getAcquire()) {
                TestTask tt = new TestTask(task, timeNanos, nextIndex(), this);
                queue.offer(tt);
                if (getAcquire()) {
                    queue.remove(tt);
                    return REJECTED;
                }
                return tt;
            }
            return REJECTED;
        }

        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (!getAcquire()) {
                TestTask tt = new TestTask(task, timeNanos + unit.toNanos(delay), nextIndex(), this);
                queue.offer(tt);
                if (getAcquire()) {
                    queue.remove(tt);
                    return REJECTED;
                }
                return tt;
            }
            return REJECTED;
        }

        @Override
        public void close() {
            if (compareAndSet(false, true)) {
                decrementWorker();
                clear(this);
            }
        }

        @Override
        public long now(TimeUnit unit) {
            return TestSchedulerService.this.now(unit);
        }
    }

    static final class TestTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable, Comparable<TestTask> {

        final long dueNanos;

        final long index;

        final Object parent;

        TestTask(Runnable run, long dueNanos, long index, Object parent) {
            this.dueNanos = dueNanos;
            this.index = index;
            this.parent = parent;
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

        @Override
        public int compareTo(TestTask o) {
            return Long.compare(index, o.index);
        }
    }
}
