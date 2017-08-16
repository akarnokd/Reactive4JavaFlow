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

import hu.akarnokd.reactive4javaflow.TestHelper;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ImmediateSchedulerServiceTest {

    @Test
    public void taskCrash() {
        TestHelper.withErrorTracking(errors -> {
            ImmediateSchedulerService.INSTANCE.schedule(() -> { throw new IllegalArgumentException(); });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test
    public void now() {
        ImmediateSchedulerService.INSTANCE.now(TimeUnit.MILLISECONDS);
    }

    @Test
    public void disposable() {
        AutoDisposable d = ImmediateSchedulerService.INSTANCE.schedule(() -> {  });

        assertEquals("ImmediatelyDone", d.toString());
        d.close();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void delayedNotSupported() {
        ImmediateSchedulerService.INSTANCE.schedule(() -> { }, 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void periodicNotSupported() {
        ImmediateSchedulerService.INSTANCE.schedulePeriodically(() -> { }, 1, 1, TimeUnit.MILLISECONDS);
    }
}
