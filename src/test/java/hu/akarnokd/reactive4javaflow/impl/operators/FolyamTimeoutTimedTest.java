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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

public class FolyamTimeoutTimedTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .timeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .timeout(1, TimeUnit.MINUTES, SchedulerServices.single())
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.timeout(1, TimeUnit.MINUTES, SchedulerServices.single()),
                IOException.class);
    }

    @Test
    public void timeoutFirst() {
        Folyam.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 0");
    }


    @Test
    public void timeoutSecond() {
        Folyam.just(1).concatWith(Folyam.never())
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 1", 1);
    }


    @Test
    public void timeoutFirstConditional() {
        Folyam.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 0");
    }


    @Test
    public void timeoutSecondConditional() {
        Folyam.just(1).concatWith(Folyam.never())
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 1", 1);
    }
}
