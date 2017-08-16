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
import org.junit.Test;

public class TrampolineSchedulerServiceTest {

    @Test
    public void taskCancel() {
        SchedulerService.Worker w = SchedulerServices.trampoline().worker();
        w.schedule(() -> {
            w.schedule(() -> { }).close();
        });
    }

    @Test
    public void taskCrash() {
        TestHelper.withErrorTracking(errors -> {
            SchedulerService.Worker w = SchedulerServices.trampoline().worker();

            w.schedule(() -> { throw new IllegalArgumentException(); });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }
}
