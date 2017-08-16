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

import static org.junit.Assert.*;

public class TestSchedulerServiceTest implements Runnable {

    TestSchedulerService scheduler = new TestSchedulerService();

    int counter;

    @Override
    public void run() {
        counter++;
    }

    @Test
    public void scheduleDirect() {
        scheduler.schedule(this);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        assertEquals(1, counter);
    }

    @Test
    public void scheduleDirectPeriodically() {
        scheduler.schedulePeriodically(this, 1, 1, TimeUnit.MILLISECONDS);
        scheduler.advanceTimeBy(3, TimeUnit.MILLISECONDS);

        assertEquals(3, counter);
    }

    @Test
    public void workerRejected() {
        SchedulerService.Worker w = scheduler.worker();
        w.close();

        assertSame(SchedulerService.REJECTED, w.schedule(this));
        assertSame(SchedulerService.REJECTED, w.schedule(this, 1, TimeUnit.MILLISECONDS));
        assertSame(SchedulerService.REJECTED, w.schedulePeriodically(this, 1, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void workerRejectedRace1() {
        for (int i = 0; i < 1000; i++) {
            SchedulerService.Worker w = scheduler.worker();

            Runnable r1 = w::close;

            Runnable r2 = () -> w.schedule(this);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void workerRejectedRace2() {
        for (int i = 0; i < 1000; i++) {
            SchedulerService.Worker w = scheduler.worker();

            Runnable r1 = w::close;

            Runnable r2 = () -> w.schedule(this, 1, TimeUnit.MILLISECONDS);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void workerRejectedRace3() {
        for (int i = 0; i < 1000; i++) {
            SchedulerService.Worker w = scheduler.worker();

            Runnable r1 = w::close;

            Runnable r2 = () -> w.schedulePeriodically(this, 1, 1, TimeUnit.MILLISECONDS);

            TestHelper.race(r1, r2);
        }
    }
}
