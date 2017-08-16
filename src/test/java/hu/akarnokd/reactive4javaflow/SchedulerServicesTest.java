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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchedulerServicesTest {
    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(SchedulerServices.class);
    }

    @Test
    public void computationHolderUtility() {
        TestHelper.checkUtilityClass(SchedulerServices.ComputationHolder.class);
    }

    @Test
    public void singleHolderUtility() {
        TestHelper.checkUtilityClass(SchedulerServices.SingleHolder.class);
    }

    @Test
    public void ioHolderUtility() {
        TestHelper.checkUtilityClass(SchedulerServices.IOHolder.class);
    }

    @Test
    public void newThreadHolderUtility() {
        TestHelper.checkUtilityClass(SchedulerServices.NewThreadHolder.class);
    }

    @Test
    public void rejected() {
        assertEquals("Rejected", SchedulerService.REJECTED.toString());
        SchedulerService.REJECTED.close();
    }

    @Test
    public void restart() throws InterruptedException {
        SchedulerServices.shutdown();
        SchedulerServices.shutdown();
        try {
            Thread.sleep(100);

            for (Thread t : Thread.getAllStackTraces().keySet()) {
                if (t.getName().contains("Reactive4JavaFlow")
                        && !t.getName().contains("Reactive4JavaFlow.Cleaner")) {
                    fail("Thread still running: " + t);
                }
            }
        } finally {
            SchedulerServices.start();
            SchedulerServices.start();
        }

        Folyam.just(1).subscribeOn(SchedulerServices.computation())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1);

        Folyam.just(1).subscribeOn(SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1);

        Folyam.just(1).subscribeOn(SchedulerServices.io())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1);

        Folyam.just(1).subscribeOn(SchedulerServices.newThread())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1);
    }

    @Test
    public void newSingle() throws InterruptedException {
        SchedulerService sch = SchedulerServices.newSingle("SchedulerServicesTest");
        try {
            Folyam.just(1).subscribeOn(sch)
                    .map(v -> Thread.currentThread().getName().contains("SchedulerServicesTest"))
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(true);
        } finally {
            sch.shutdown();
        }

        Thread.sleep(100);

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("SchedulerServicesTest")) {
                fail("Thread still running: " + t);
            }
        }

    }


    @Test
    public void newThread() throws InterruptedException {
        SchedulerService sch = SchedulerServices.newThread("SchedulerServicesTest");
        try {
            Folyam.just(1).subscribeOn(sch)
                    .map(v -> Thread.currentThread().getName().contains("SchedulerServicesTest"))
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(true);
        } finally {
            sch.shutdown();
        }

        Thread.sleep(100);

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("SchedulerServicesTest")) {
                fail("Thread still running: " + t);
            }
        }
    }

    @Test
    public void newParallel() throws InterruptedException {
        SchedulerService sch = SchedulerServices.newParallel(2, "SchedulerServicesTest");
        try {
            Folyam.range(1, 2).subscribeOn(sch)
                    .parallel(2)
                    .map(v -> Thread.currentThread().getName().contains("SchedulerServicesTest"))
                    .sequential()
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(true, true);
        } finally {
            sch.shutdown();
        }

        Thread.sleep(100);

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("SchedulerServicesTest")) {
                fail("Thread still running: " + t);
            }
        }
    }
}
