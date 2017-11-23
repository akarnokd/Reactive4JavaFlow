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

import hu.akarnokd.reactive4javaflow.disposables.BooleanAutoDisposable;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertSame;

public class SchedulerServiceTest {

    SchedulerService sch = () -> new SchedulerService.Worker() {
        volatile boolean closed;
        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (closed) {
                return SchedulerService.REJECTED;
            }
            task.run();
            return new BooleanAutoDisposable();
        }

        @Override
        public void close() {
            closed = true;
        }
    };

    SchedulerService schRejecting = () -> new SchedulerService.Worker() {
        volatile boolean closed;
        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            return SchedulerService.REJECTED;
        }

        @Override
        public void close() {
            closed = true;
        }
    };

    @Test
    public void schedulerDirectCrash() {
        TestHelper.withErrorTracking(errors ->{
            sch.schedule(() -> { throw new IllegalArgumentException(); });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void schedulerDirectPeriodicCrash() {
        TestHelper.withErrorTracking(errors ->{
            sch.schedulePeriodically(() -> { throw new IllegalArgumentException(); }, 1, 1, TimeUnit.MILLISECONDS);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void schedulerDirectPeriodicRejected() {
        assertSame(SchedulerService.REJECTED,
                schRejecting.schedulePeriodically(
                        () -> { throw new IllegalArgumentException(); }, 1, 1, TimeUnit.MILLISECONDS));
    }

}
