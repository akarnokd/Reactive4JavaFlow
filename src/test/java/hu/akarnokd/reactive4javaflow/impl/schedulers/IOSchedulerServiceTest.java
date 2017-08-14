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
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class IOSchedulerServiceTest {

    void sleep() throws InterruptedException {
        if (System.getenv("CI") == null) {
            Thread.sleep(100);
        } else {
            Thread.sleep(1000);
        }
    }

    @Test
    public void normal() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        SchedulerServices.io().schedule(tc::onComplete);

        tc.awaitDone(5, TimeUnit.SECONDS)
                .assertResult();
    }

    @Test
    public void normal2() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        SchedulerServices.io().schedule(tc::onComplete, 10, TimeUnit.MILLISECONDS);

        tc.awaitDone(5, TimeUnit.SECONDS)
                .assertResult();
    }

    @Test
    public void range() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 5)
                    .subscribeOn(SchedulerServices.io())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1, 2, 3, 4, 5);
        }
    }

    @Test
    public void newIO() throws Exception {
        IOSchedulerService io = (IOSchedulerService)SchedulerServices.newIO("TestingIOSchedulers");
        try {

            assertEquals(0, io.size());

            Folyam.range(1, 5)
                    .observeOn(io, 1)
                    .take(3)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1, 2, 3);

            assertEquals(1, io.size());

            Folyam.range(1, 5)
                    .observeOn(io, 1)
                    .take(3)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(1, 2, 3);

            assertEquals(1, io.size());

            AutoDisposable d1 = io.worker();
            AutoDisposable d2 = io.worker();

            assertEquals(2, io.size());

            d1.close();
            d2.close();

            assertEquals(2, io.size());

            io.clear();

            assertEquals(0, io.size());
        } finally {
            io.shutdown();
        }

        sleep();

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("TestingIOSchedulers")) {
                throw new AssertionError("Thread alive? " + t);
            }
        }
    }

    @Test
    public void newIOShutdown() throws Exception {
        IOSchedulerService io = (IOSchedulerService)SchedulerServices.newIO("TestingIOSchedulers");

        io.shutdown();

        io.start();

        Folyam.range(1, 5)
                .subscribeOn(io)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3, 4, 5);

        io.shutdown();

        sleep();

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("TestingIOSchedulers")) {
                throw new AssertionError("Thread alive? " + t);
            }
        }

        TestHelper.withErrorTracking(errors -> {
            assertSame(SchedulerService.REJECTED, io.schedule(() -> { }));

            assertSame(SchedulerService.REJECTED, io.worker().schedule(() -> { }));

            TestHelper.assertError(errors, 0, RejectedExecutionException.class);
            TestHelper.assertError(errors, 1, RejectedExecutionException.class);
        });
    }
}
