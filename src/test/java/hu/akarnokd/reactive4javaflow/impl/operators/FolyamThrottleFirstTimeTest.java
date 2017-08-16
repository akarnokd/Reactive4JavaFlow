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
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FolyamThrottleFirstTimeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).throttleFirst(0, TimeUnit.MILLISECONDS, SchedulerServices.single()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.throttleFirst(0, TimeUnit.MILLISECONDS, SchedulerServices.single()),
                IOException.class
            );
    }

    @Test
    public void normal() {
        TestSchedulerService executor = new TestSchedulerService();

        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.throttleFirst(10, TimeUnit.MILLISECONDS, executor).test();

        dp.onNext(1);

        tc.assertValues(1);

        dp.onNext(2);

        tc.assertValues(1);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(1);

        dp.onNext(3);

        tc.assertValues(1, 3);

        dp.onNext(4);

        tc.assertValues(1, 3);

        dp.onComplete();

        tc.assertResult(1, 3);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertResult(1, 3);
    }

    @Test
    public void normalConditional() {
        TestSchedulerService executor = new TestSchedulerService();

        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.throttleFirst(10, TimeUnit.MILLISECONDS, executor)
                .filter(v -> true)
                .test();

        dp.onNext(1);

        tc.assertValues(1);

        dp.onNext(2);

        tc.assertValues(1);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertValues(1);

        dp.onNext(3);

        tc.assertValues(1, 3);

        dp.onNext(4);

        tc.assertValues(1, 3);

        dp.onComplete();

        tc.assertResult(1, 3);

        executor.advanceTimeBy(10, TimeUnit.MILLISECONDS);

        tc.assertResult(1, 3);
    }
}
