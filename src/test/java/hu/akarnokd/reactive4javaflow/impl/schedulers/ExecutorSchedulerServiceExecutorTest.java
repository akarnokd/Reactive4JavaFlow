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
import org.junit.*;

import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class ExecutorSchedulerServiceExecutorTest extends AbstractSchedulerServiceTest {

    static ExecutorService exec;

    @BeforeClass
    public static void beforeClass() {
        exec = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void afterClass() {
        exec.shutdown();
    }

    @Override
    protected SchedulerService create() {
        return SchedulerServices.newExecutor(exec, false);
    }

    @Test
    public void rejected() {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.shutdown();

        ExecutorSchedulerService.shutdownTimedHelpers();
        try {
            SchedulerService sch = SchedulerServices.newExecutor(exec, false);

            TestHelper.withErrorTracking(errors -> {
                assertSame(SchedulerService.REJECTED, sch.schedule(() -> { }));

                assertSame(SchedulerService.REJECTED, sch.schedule(() -> { }, 1, TimeUnit.DAYS));

                try (SchedulerService.Worker w = sch.worker()) {
                    assertSame(SchedulerService.REJECTED, w.schedule(() -> { }));

                    assertSame(SchedulerService.REJECTED, w.schedule(() -> { }, 1, TimeUnit.DAYS));
                }

                assertFalse(errors.isEmpty());
                for (int i = 0; i < errors.size(); i++) {
                    TestHelper.assertError(errors, i, RejectedExecutionException.class);
                }
            });
        } finally {
            ExecutorSchedulerService.startTimedHelpers();
        }
    }

    @Test
    public void rejectedTrampolined() {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.shutdown();
        ExecutorSchedulerService.shutdownTimedHelpers();
        try {

            SchedulerService sch = SchedulerServices.newExecutor(exec, true);

            TestHelper.withErrorTracking(errors -> {
                assertSame(SchedulerService.REJECTED, sch.schedule(() -> {
                }));

                assertSame(SchedulerService.REJECTED, sch.schedule(() -> {
                }, 1, TimeUnit.DAYS));

                try (SchedulerService.Worker w = sch.worker()) {
                    assertSame(SchedulerService.REJECTED, w.schedule(() -> { }));

                    assertSame(SchedulerService.REJECTED, w.schedule(() -> { }, 1, TimeUnit.DAYS));
                }

                assertFalse(errors.isEmpty());
                for (int i = 0; i < errors.size(); i++) {
                    TestHelper.assertError(errors, i, RejectedExecutionException.class);
                }
            });
        } finally {
            ExecutorSchedulerService.startTimedHelpers();
        }
    }
}
