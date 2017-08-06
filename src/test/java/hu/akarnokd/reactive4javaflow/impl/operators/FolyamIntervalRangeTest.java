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

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class FolyamIntervalRangeTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single()),
                0L, 1L, 2L, 3L, 4L
        );
    }

    @Test
    public void standard1Conditional() {
        TestHelper.assertResult(
                Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                        .filter(v -> true),
                0L, 1L, 2L, 3L, 4L
        );
    }

    @Test
    public void normal() {
        Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test
    public void normalConditional() {
        Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    public void normal2() {
        Folyam.intervalRange(10, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(10L, 11L, 12L, 13L, 14L);
    }


    @Test
    public void normalConditional2() {
        Folyam.intervalRange(10, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(10L, 11L, 12L, 13L, 14L);
    }

    @Test(timeout = 10000)
    public void trampoline() {
        Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                .test()
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test(timeout = 10000)
    public void trampolineConditional() {
        Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                .filter(v -> true)
                .test()
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }


    @Test(timeout = 10000)
    public void normalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void normalConditionalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void fusedConditionalLoop() {
        for (int i = 0; i < 50; i++) {
            Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .filter(v -> true)
                    .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test(timeout = 10000)
    public void fusedConditionalLoopTrampoline() {
        for (int i = 0; i < 50; i++) {
            Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.trampoline())
                    .filter(v -> true)
                    .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertResult(0L, 1L, 2L, 3L, 4L);

        }
    }

    @Test
    public void requestOneByOne() {
        TestConsumer<Long> ts = new TestConsumer<>(0);

        Folyam.intervalRange(0, 5, 1, 1, TimeUnit.MILLISECONDS, SchedulerServices.single())
        .subscribe(ts);

        for (int i = 0; i < 5; i++) {
            ts.requestMore(1)
                    .awaitCount(i + 1, 10, 5000)
                    .assertNoTimeout()
                    .assertValueCount(i + 1)
            ;
        }

        ts.awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0L, 1L, 2L, 3L, 4L);
    }
}
