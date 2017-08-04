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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class FolyamTimerTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single()), 0L);
    }

    @Test(timeout = 1000)
    public void normal() {
        Folyam.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single())
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(0L);
    }

    @Test(timeout = 1000)
    public void trampolined() {
        Folyam.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L);
    }
}
