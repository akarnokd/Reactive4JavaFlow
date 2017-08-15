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
import java.util.concurrent.TimeUnit;

public class EsetlegTimeoutTimedFallbackTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .timeout(1, TimeUnit.MINUTES, SchedulerServices.single(), Esetleg.just(6))
                , 1
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .timeout(1, TimeUnit.MINUTES, SchedulerServices.single(), Esetleg.just(6))
                , 1
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.timeout(1, TimeUnit.MINUTES, SchedulerServices.single(), Esetleg.just(6)),
                IOException.class);
    }

    @Test
    public void timeoutFirst() {
        Esetleg.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.just(6))
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(6);
    }


    @Test
    public void timeoutSecond() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.just(6))
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 6);
    }


    @Test
    public void timeoutFirstConditional() {
        Esetleg.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.just(6))
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(6);
    }


    @Test
    public void timeoutSecondConditional() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.just(6))
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 6);
    }

    @Test
    public void fallbackError() {
        Esetleg.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.error(new IOException()))
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }

    @Test
    public void fallbackErrorConditional() {
        Esetleg.never()
                .timeout(10, TimeUnit.MILLISECONDS, SchedulerServices.single(), Esetleg.error(new IOException()))
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class);
    }
}
