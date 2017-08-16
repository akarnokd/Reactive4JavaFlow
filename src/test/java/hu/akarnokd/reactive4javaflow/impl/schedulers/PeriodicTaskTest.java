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
import hu.akarnokd.reactive4javaflow.disposables.BooleanAutoDisposable;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PeriodicTaskTest {

    @Test
    public void taskCrash() {
        TestHelper.withErrorTracking(errors -> {
            SchedulerService.Worker worker = SchedulerServices.trampoline().worker();

            PeriodicTask pt = new PeriodicTask(worker, () -> {
                throw new IllegalArgumentException();
            }, 1, TimeUnit.MILLISECONDS, System.currentTimeMillis());

            pt.run();

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void close() {
        SchedulerService.Worker worker = SchedulerServices.trampoline().worker();

        PeriodicTask pt = new PeriodicTask(worker, () -> {
            throw new IllegalArgumentException();
        }, 1, TimeUnit.MILLISECONDS, System.currentTimeMillis());

        BooleanAutoDisposable d = new BooleanAutoDisposable();
        pt.setFirst(d);

        assertFalse(d.isClosed());

        pt.close();

        assertTrue(d.isClosed());
    }
}
