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
import org.junit.*;

import java.lang.ref.WeakReference;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public abstract class AbstractSchedulerServiceTest {

    SchedulerService scheduler;

    @Before
    public void before() {
        scheduler = create();
    }

    @After
    public void after() {
        release(scheduler);
    }

    protected abstract SchedulerService create();

    protected void release(SchedulerService service) {
        // optionally release the scheduler
    }

    @Test(timeout = 5000)
    public void directImmediate() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);

        scheduler.schedule(cdl::countDown);

        assertTrue(cdl.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 5000)
    public void directDelayed() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);

        scheduler.schedule(cdl::countDown, 1, TimeUnit.MILLISECONDS);

        assertTrue(cdl.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 5000)
    public void directPeriodic() throws Exception {
        CountDownLatch cdl = new CountDownLatch(4);

        AutoDisposable d = scheduler.schedulePeriodically(cdl::countDown, 1, 1, TimeUnit.MILLISECONDS);

        assertTrue(cdl.await(5, TimeUnit.SECONDS));

        d.close();
    }

    @Test(timeout = 5000)
    public void workerImmediate() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);

        try (SchedulerService.Worker w = scheduler.worker()) {
            w.schedule(cdl::countDown);

            assertTrue(cdl.await(5, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 5000)
    public void workerDelayed() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);

        try (SchedulerService.Worker w = scheduler.worker()) {

            w.schedule(cdl::countDown, 1, TimeUnit.MILLISECONDS);

            assertTrue(cdl.await(5, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 5000)
    public void workerPeriodic() throws Exception {
        CountDownLatch cdl = new CountDownLatch(4);

        try (SchedulerService.Worker w = scheduler.worker()) {
            try (AutoDisposable d = w.schedulePeriodically(cdl::countDown, 1, 1, TimeUnit.MILLISECONDS)) {

                assertTrue(cdl.await(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeout = 5000)
    public void workerRejectedImmediately() {
        SchedulerService.Worker w = scheduler.worker();
        w.close();

        assertSame(SchedulerService.REJECTED, w.schedule(() -> { }));
    }

    @Test(timeout = 5000)
    public void workerRejectedDelayed() {
        SchedulerService.Worker w = scheduler.worker();
        w.close();

        assertSame(SchedulerService.REJECTED, w.schedule(() -> { }, 1, TimeUnit.MILLISECONDS));
    }

    @Test(timeout = 5000)
    public void workerRejectedPeriodic() {
        SchedulerService.Worker w = scheduler.worker();
        w.close();

        assertSame(SchedulerService.REJECTED, w.schedulePeriodically(() -> { }, 1, 1, TimeUnit.MILLISECONDS));
    }

    @Test(timeout = 5000)
    public void workerDelayedNoRetention() throws InterruptedException {
        Integer j = 12345678;
        WeakReference<Integer> wr = new WeakReference<>(j);

        try (SchedulerService.Worker w = scheduler.worker()) {
            for (int i = 0; i < 10000; i++) {
                final Integer k = j;
                w.schedule(() -> {
                    System.out.println(k);
                    System.out.println(wr);
                }, 1, TimeUnit.DAYS);
            }
        }

        j = null;
        System.gc();
        Thread.sleep(200);

        assertNull(wr.get());
    }

    @Test(timeout = 5000)
    public void workerPeriodicNoRetention() throws InterruptedException {
        Integer j = 12345678;
        WeakReference<Integer> wr = new WeakReference<>(j);

        try (SchedulerService.Worker w = scheduler.worker()) {
            for (int i = 0; i < 10000; i++) {
                final Integer k = j;
                w.schedulePeriodically(() -> {
                    System.out.println(k);
                    System.out.println(wr);
                }, 1, 1, TimeUnit.DAYS);
            }
        }

        j = null;
        System.gc();
        Thread.sleep(200);

        assertNull(wr.get());
    }

    @Test(timeout = 5000)
    public void directImmediateCancel() {
        scheduler.schedule(() -> { }).close();
    }

    @Test(timeout = 5000)
    public void directDelayedCancel() {
        scheduler.schedule(() -> { }, 1, TimeUnit.DAYS).close();
    }

    @Test(timeout = 5000)
    public void directPeriodicCancel() {
        scheduler.schedulePeriodically(() -> { }, 1, 1, TimeUnit.DAYS).close();
    }


    @Test(timeout = 5000)
    public void workerImmediateCancel() {
        try (SchedulerService.Worker w = scheduler.worker()) {
            w.schedule(() -> { }).close();
        }
    }

    @Test(timeout = 5000)
    public void workerDelayedCancel() {
        try (SchedulerService.Worker w = scheduler.worker()) {
            w.schedule(() -> {
            }, 1, TimeUnit.DAYS).close();
        }
    }

    @Test(timeout = 5000)
    public void workerPeriodicCancel() {
        try (SchedulerService.Worker w = scheduler.worker()) {
            w.schedulePeriodically(() -> {
            }, 1, 1, TimeUnit.DAYS).close();
        }
    }


    @Test(timeout = 5000)
    public void directImmediateCrash() throws Exception {
        TestHelper.withErrorTracking(errors -> {
            scheduler.schedule(() -> { throw new IllegalArgumentException(); });

            Thread.sleep(100);

            assertFalse(errors.isEmpty());
            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test(timeout = 50000)
    public void directDelayedCrash() throws Exception {
        TestHelper.withErrorTracking(errors -> {
            scheduler.schedule(() -> { throw new IllegalArgumentException(); }, 1, TimeUnit.MILLISECONDS);

            Thread.sleep(100);

            assertFalse(errors.isEmpty());
            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test(timeout = 5000)
    public void directPeriodicCrash() throws Exception {
        TestHelper.withErrorTracking(errors -> {
            scheduler.schedulePeriodically(() -> {
                throw new IllegalArgumentException();
            }, 1, 1, TimeUnit.MILLISECONDS);

            Thread.sleep(100);

            assertFalse(errors.isEmpty());
            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test(timeout = 5000)
    public void workerImmediateCrash() throws Exception {
        try (SchedulerService.Worker w = scheduler.worker()) {
            TestHelper.withErrorTracking(errors -> {
                w.schedule(() -> { throw new IllegalArgumentException(); });

                Thread.sleep(100);

                assertFalse(errors.isEmpty());
                TestHelper.assertError(errors, 0, IllegalArgumentException.class);
            });
        }
    }

    @Test(timeout = 5000)
    public void workerDelayedCrash() throws Exception {
        try (SchedulerService.Worker w = scheduler.worker()) {
            TestHelper.withErrorTracking(errors -> {
                scheduler.schedule(() -> { throw new IllegalArgumentException(); }, 1, TimeUnit.MILLISECONDS);

                Thread.sleep(100);

                assertFalse(errors.isEmpty());
                TestHelper.assertError(errors, 0, IllegalArgumentException.class);
            });
        }
    }

    @Test(timeout = 5000)
    public void workerPeriodicCrash() throws Exception {
        TestHelper.withErrorTracking(errors -> {
            scheduler.schedulePeriodically(() -> { throw new IllegalArgumentException(); }, 1, 1, TimeUnit.MILLISECONDS);

            Thread.sleep(100);

            assertFalse(errors.isEmpty());
            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }
}
