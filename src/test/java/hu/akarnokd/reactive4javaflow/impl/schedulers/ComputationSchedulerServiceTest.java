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
import hu.akarnokd.reactive4javaflow.disposables.SequentialAutoDisposable;
import org.junit.*;

import java.util.concurrent.*;

import static org.junit.Assert.*;

public class ComputationSchedulerServiceTest extends AbstractSchedulerServiceTest {

    @Override
    protected SchedulerService create() {
        return SchedulerServices.computation();
    }

    @Test
    public void shutdownDirectRejects() {
        TestHelper.withErrorTracking(errors -> {
            SchedulerService sch = SchedulerServices.newParallel(1, "A");
            sch.shutdown();

            assertSame(SchedulerService.REJECTED, sch.schedule(() -> { }));

            assertSame(SchedulerService.REJECTED, sch.schedule(() -> { }, 1, TimeUnit.MILLISECONDS));

            assertSame(SchedulerService.REJECTED, sch.schedulePeriodically(() -> { }, 1, 1, TimeUnit.MILLISECONDS));

            assertFalse(errors.isEmpty());

            for (int i = 0; i < errors.size(); i++) {
                TestHelper.assertError(errors, i, RejectedExecutionException.class);
            }
        });
    }

    @Test(timeout = 5000)
    public void zeroPeriod() throws InterruptedException {
        CountDownLatch cdl = new CountDownLatch(4);

        SequentialAutoDisposable d = new SequentialAutoDisposable();

        d.replace(SchedulerServices.computation().schedulePeriodically(() -> {
            cdl.countDown();
            if (cdl.getCount() == 0) {
                d.close();
            }
        }, 1, 0, TimeUnit.MILLISECONDS));

        assertTrue(cdl.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void startRace() {
        SchedulerService sch = SchedulerServices.newParallel(1, "A");
        sch.shutdown();

        for (int i = 0; i < 1000; i++) {

            TestHelper.race(sch::start, sch::start);

            sch.shutdown();
        }
    }

}
