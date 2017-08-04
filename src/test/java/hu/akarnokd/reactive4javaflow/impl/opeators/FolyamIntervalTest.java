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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class FolyamIntervalTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .take(5),
                0L, 1L, 2L, 3L, 4L
        );
    }

    @Test
    public void standard1Conditional() {
        TestHelper.assertResult(
                Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                        .filter(v -> true)
                        .take(5),
                0L, 1L, 2L, 3L, 4L
        );
    }

    @Test
    public void normal() {
        Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .take(5)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test
    public void normal2() {
        Folyam.interval(1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .take(5)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test
    public void normalConditional() {
        Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .filter(v -> true)
                .take(5)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }

    @Test(timeout = 10000)
    public void trampoline() {
        Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                .take(5)
                .test()
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test(timeout = 10000)
    public void trampolineConditional() {
        Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                .filter(v -> true)
                .take(5)
                .test()
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test(timeout = 10000)
    public void normalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .take(5)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void normalConditionalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .take(5)
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void fusedConditionalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .filter(v -> true)
                    .take(5)
                    .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void fusedConditionalLoopTrampoline() {
        for (int i = 0; i < 50; i++) {
            Folyam.interval(1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                    .filter(v -> true)
                    .take(5)
                    .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }
}
