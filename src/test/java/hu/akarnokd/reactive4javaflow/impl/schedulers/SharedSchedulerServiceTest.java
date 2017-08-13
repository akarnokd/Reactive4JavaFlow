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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.BooleanAutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.schedulers.SharedSchedulerService.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.SharedSchedulerService.SharedWorker.*;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class SharedSchedulerServiceTest implements Runnable {

    volatile int calls;

    @Override
    public void run() {
        calls++;
    }

    @Test(timeout = 5000)
    public void normal() {
        SharedSchedulerService scheduler = new SharedSchedulerService(SchedulerServices.newThread().worker());
        try {
            final Set<String> threads = new HashSet<>();

            for (int i = 0; i < 100; i++) {
                Folyam.just(1).subscribeOn(scheduler)
                        .map((CheckedFunction<Integer, Object>) v -> threads.add(Thread.currentThread().getName()))
                        .blockingLast();
            }

            assertEquals(1, threads.size());
        } finally {
            scheduler.shutdown();
        }
    }

    @Test(timeout = 5000)
    public void delay() {
        SchedulerService scheduler = SchedulerServices.newShared(SchedulerServices.newThread().worker());
        try {
            final Set<String> threads = new HashSet<>();

            for (int i = 0; i < 100; i++) {
                Folyam.just(1).delay(1, TimeUnit.MILLISECONDS, scheduler)
                        .map((CheckedFunction<Integer, Object>) v -> threads.add(Thread.currentThread().getName()))
                        .blockingLast();
            }

            assertEquals(1, threads.size());
        } finally {
            scheduler.shutdown();
        }
    }

    long memoryUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    @Test
    public void noleak() throws Exception {
        SharedSchedulerService scheduler = new SharedSchedulerService(SchedulerServices.newThread());
        try {
            SchedulerService.Worker worker = scheduler.worker();

            worker.schedule(() -> { });

            System.gc();
            Thread.sleep(500);

            long before = memoryUsage();
            System.out.printf("Start: %.1f%n", before / 1024.0 / 1024.0);

            for (int i = 0; i < 200 * 1000; i++) {
                worker.schedule(() -> { }, 1, TimeUnit.DAYS);
            }

            long middle = memoryUsage();

            System.out.printf("Middle: %.1f -> %.1f%n", before / 1024.0 / 1024.0, middle / 1024.0 / 1024.0);

            worker.close();

            System.gc();

            Thread.sleep(100);

            int wait = 400;

            long after = memoryUsage();

            while (wait-- > 0) {
                System.out.printf("Usage: %.1f -> %.1f -> %.1f%n", before / 1024.0 / 1024.0, middle / 1024.0 / 1024.0, after / 1024.0 / 1024.0);

                if (middle > after * 2) {
                    return;
                }

                Thread.sleep(100);

                System.gc();

                Thread.sleep(100);

                after = memoryUsage();
            }

            fail(String.format("Looks like there is a memory leak: %.1f -> %.1f -> %.1f", before / 1024.0 / 1024.0, middle / 1024.0 / 1024.0, after / 1024.0 / 1024.0));

        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void now() {
        TestSchedulerService test = new TestSchedulerService();

        SharedSchedulerService scheduler = new SharedSchedulerService(test);

        assertEquals(0L, scheduler.now(TimeUnit.MILLISECONDS));

        assertEquals(0L, scheduler.worker().now(TimeUnit.MILLISECONDS));
    }

    @Test
    public void direct() {
        TestSchedulerService test = new TestSchedulerService();

        SharedSchedulerService scheduler = new SharedSchedulerService(test);

        scheduler.schedule(this);

        test.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        assertEquals(1, calls);

        scheduler.schedule(this, 1, TimeUnit.MILLISECONDS);

        test.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        assertEquals(2, calls);

        scheduler.schedulePeriodically(this, 1, 1, TimeUnit.MILLISECONDS);

        test.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        assertEquals(12, calls);

        SchedulerService.Worker worker = scheduler.worker();
        worker.close();

        assertSame(SchedulerService.REJECTED, worker.schedule(this));

        assertSame(SchedulerService.REJECTED, worker.schedule(this, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void taskCrash() {
        TestSchedulerService test = new TestSchedulerService();

        SharedSchedulerService scheduler = new SharedSchedulerService(test);

        AutoDisposable d = scheduler.worker().schedule(() -> {
            throw new IllegalArgumentException();
        });

//        assertFalse(d.isDisposed());

        try {
            test.advanceTimeBy(0, TimeUnit.SECONDS);
        } catch (IllegalArgumentException ex) {
            // expected
        }

        assertNotSame(SchedulerService.REJECTED, d);
//        assertTrue(d.isDisposed());
    }

    @Test(timeout = 5000)
    public void futureDisposeRace() throws Exception {
        SharedSchedulerService scheduler = new SharedSchedulerService(SchedulerServices.computation());
        try {
            SchedulerService.Worker w = scheduler.worker();
            for (int i = 0; i < 1000; i++) {
                w.schedule(this);
            }

            while (calls != 1000) {
                Thread.sleep(100);
            }
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void disposeSetFutureRace() {
        TestSchedulerService test = new TestSchedulerService();

        SharedSchedulerService scheduler = new SharedSchedulerService(test);

        SharedWorker worker = (SharedWorker)scheduler.worker();

        for (int i = 0; i < 1000; i++) {
            final SharedAction sa = new SharedAction(this, worker);
            worker.add(sa);

            final BooleanAutoDisposable d = new BooleanAutoDisposable();

            Runnable r1 = () -> sa.setFuture(d);

            Runnable r2 = sa::close;

            TestHelper.race(r1, r2);

            assertTrue("Future not disposed", d.isClosed());
        }
    }

    @Test
    public void runSetFutureRace() {
        TestSchedulerService test = new TestSchedulerService();

        SharedSchedulerService scheduler = new SharedSchedulerService(test);

        SharedWorker worker = (SharedWorker)scheduler.worker();

        for (int i = 0; i < 1000; i++) {
            final SharedAction sa = new SharedAction(this, worker);
            worker.add(sa);

            final BooleanAutoDisposable d = new BooleanAutoDisposable();

            Runnable r1 = () -> sa.setFuture(d);

            TestHelper.race(r1, sa);

            assertFalse("Future disposed", d.isClosed());
            assertEquals(i + 1, calls);
        }
    }
}
